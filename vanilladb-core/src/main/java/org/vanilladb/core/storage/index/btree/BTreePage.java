package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

/**
 * A page corresponding to a single B-tree block in a file for {@link BTreeDir}
 * or {@link BTreeLeaf}.
 * <p>
 * The content of each B-tree block begins with an integer storing the number of
 * index records in that page, then a series of integer flags, followed by a
 * series of slots holding index records. Index records are sorted in ascending
 * order.
 * </p>
 */
public class BTreePage {
	private TableInfo ti;
	private BlockId blk;
	private Transaction tx;
	private int slotSize, headerSize;
	private String dataFileName;
	private Buffer currentBuff;
	private Map<String, Integer> myOffsetMap;
	// Optimization: Materialize the number of records of B-Tree Page.
	private int numberOfRecords;

	// Optimization: Materialize the offset map.
	/**
	 * Returns the offset of a specified field within a record.
	 * 
	 * @param fldName the name of the field
	 * 
	 * @return the offset of that field within a record
	 */
	@Deprecated
	public static int offset(Schema sch, String fldName) {
		int pos = 0;
		for (String fldname : sch.fields()) {
			if (fldName.equals(fldname))
				break;
			pos += Page.maxSize(sch.type(fldname));
		}
		return pos;
	}

	/**
	 * Returns the map of field name to offset of a specified schema.
	 * 
	 * @param sch
	 *            the table's schema
	 * 
	 * @return the offset map
	 */
	public static Map<String, Integer> offsetMap(Schema sch) {
		int pos = 0;
		Map<String, Integer> offsetMap = new HashMap<String, Integer>();

		for (String fldname : sch.fields()) {
			offsetMap.put(fldname, pos);
			pos += Page.maxSize(sch.type(fldname));
		}
		return offsetMap;
	}

	/**
	 * Returns the number of bytes required to store a record in disk.
	 * 
	 * @return the size of a record, in bytes
	 */
	public static int slotSize(Schema sch) {
		int pos = 0;
		for (String fldname : sch.fields())
			pos += Page.maxSize(sch.type(fldname));
		return pos;
	}

	/**
	 * Opens a page for the specified B-tree block.
	 * 
	 * @param dataFileName
	 *            the data file name
	 * @param blk
	 *            a block ID refers to the B-tree block
	 * @param numFlags
	 *            the number of flags in this b-tree page
	 * @param ti
	 *            the metadata for the particular B-tree file
	 * @param tx
	 *            the calling transaction
	 */
	public BTreePage(String dataFileName, BlockId blk, int numFlags,
			TableInfo ti, Transaction tx) {
		this.dataFileName = dataFileName;
		this.blk = blk;
		this.ti = ti;
		this.tx = tx;
		currentBuff = VanillaDb.bufferMgr().pin(blk, tx.getTransactionNumber());
		slotSize = slotSize(ti.schema());
		headerSize = Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * numFlags;
		myOffsetMap = offsetMap(ti.schema());
		numberOfRecords = -1;
	}

	/**
	 * Closes the page by unpinning its buffer.
	 */
	public void close() {
		if (blk != null) {
			VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), currentBuff);
			blk = null;
			currentBuff = null;
			numberOfRecords = -1;
		}
	}

	/**
	 * Returns the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @return the i-th flag
	 */
	public long getFlag(int i) {
		return (Long) getVal(Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i,
				BIGINT).asJavaVal();
	}

	/**
	 * Sets the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @param val
	 *            the flag value
	 * @return the i-th flag
	 */
	public void setFlag(int i, long val) {
		int offset = Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i;
		Constant v = new BigIntConstant(val);
		setVal(offset, v);
	}

	public Constant getVal(int slot, String fldName) {
		Type type = ti.schema().type(fldName);
		return getVal(fieldPosition(slot, fldName), type);
	}

	public void setVal(int slot, String fldName, Constant val) {
		Type type = ti.schema().type(fldName);
		Constant v = val.castTo(type);
		setVal(fieldPosition(slot, fldName), v);
	}

	public void insert(int slot) {
		for (int i = getNumRecords(); i > slot; i--)
			copyRecord(i - 1, i);
		setNumRecords(getNumRecords() + 1);
	}

	public void delete(int slot) {
		for (int i = slot + 1; i < getNumRecords(); i++)
			copyRecord(i, i - 1);
		setNumRecords(getNumRecords() - 1);
		return;
	}

	/**
	 * Returns true if the block is full.
	 * 
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotPosition(getNumRecords() + 1) >= BLOCK_SIZE;
	}

	/**
	 * Returns true if the block is going to be full after insertion.
	 * 
	 * @return true if the block is going to be full after insertion
	 */
	public boolean isGettingFull() {
		return slotPosition(getNumRecords() + 2) >= BLOCK_SIZE;
	}

	/**
	 * Splits the page at the specified slot. A new page is created, and the
	 * records of the page starting from the split slot are transferred to the
	 * new page.
	 * 
	 * @param splitSlot
	 *            the split position
	 * @param flags
	 *            the flag values
	 * @return the number of the new block
	 */
	public long split(int splitSlot, long[] flags) {
		BlockId newBlk = appendBlock(flags);
		BTreePage newPage = new BTreePage(dataFileName, newBlk, flags.length,
				ti, tx);
		transferRecords(splitSlot, newPage, 0, getNumRecords() - splitSlot);
		newPage.close();
		return newBlk.number();
	}

	public void transferRecords(int start, BTreePage dest, int destStart,
			int num) {
		// not deal with the problem that the transfer data is larger than a
		// block
		int numToTransfer = Math.min(getNumRecords() - start, num);
		for (int i = 0; i < numToTransfer; i++) {
			dest.insert(destStart + i);
			Schema sch = ti.schema();
			for (String fldname : sch.fields())
				dest.setVal(destStart + i, fldname, getVal(start, fldname));
			delete(start);
		}
	}

	public BlockId currentBlk() {
		return blk;
	}

	/**
	 * Returns the number of index records in this page.
	 * 
	 * @return the number of index records in this page
	 */
	public int getNumRecords() {
		// return (Integer) getVal(0, INTEGER).asJavaVal();
		// Optimization:
		if (numberOfRecords == -1)
			numberOfRecords = (Integer) getVal(0, INTEGER).asJavaVal();
		return numberOfRecords;
	}

	private void setNumRecords(int n) {
		Constant v = new IntegerConstant(n);
		setVal(0, v);
		// Optimization:
		numberOfRecords = n;
	}

	private void copyRecord(int from, int to) {
		Schema sch = ti.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}

	private int fieldPosition(int slot, String fldname) {
		int offset = myOffsetMap.get(fldname);
		return slotPosition(slot) + offset;
	}

	private int slotPosition(int slot) {
		return headerSize + (slot * slotSize);
	}

	private BlockId appendBlock(long[] flags) {
		try {
			tx.concurrencyMgr().modifyFile(ti.fileName());
			BTPageFormatter btpf = new BTPageFormatter(ti, flags);
			Buffer buff = VanillaDb.bufferMgr().pinNew(ti.fileName(), btpf,
					tx.getTransactionNumber());
			VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);
			return buff.block();
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private void setVal(int offset, Constant val) {
		currentBuff.setVal(offset, val, tx.getTransactionNumber(), -1);
	}

	private Constant getVal(int offset, Type type) {
		return currentBuff.getVal(offset, type);
	}
}
