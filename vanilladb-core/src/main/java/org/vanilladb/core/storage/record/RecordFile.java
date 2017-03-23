package org.vanilladb.core.storage.record;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.SchemaIncompatibleException;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

/**
 * Manages a file of records. There are methods for iterating through the
 * records and accessing their contents. Note that the first block (block 0) of
 * a record file is reserved for file header, and the actual data block is start
 * from block 1.
 * 
 * <p>
 * The {@link #insert()} method must be called before setters.
 * </p>
 * 
 * <p>
 * The {@link #beforeFirst()} method must be called before {@link #next()}.
 * </p>
 */
public class RecordFile implements Record {
	private BlockId headerBlk;
	private TableInfo ti;
	private Transaction tx;
	private String fileName;
	private RecordPage rp;
	private FileHeaderPage fhp;
	private long currentBlkNum;
	private boolean doLog;

	/**
	 * Constructs an object to manage a file of records. If the file does not
	 * exist, it is created. This method should be called by {@link TableInfo}
	 * only. To obtain an instance of this class, call
	 * {@link TableInfo#open(Transaction)} instead.
	 * 
	 * @param ti
	 *            the table metadata
	 * @param tx
	 *            the transaction
	 * @param doLog
	 *            true if the underlying record modification should perform
	 *            logging
	 */
	public RecordFile(TableInfo ti, Transaction tx, boolean doLog) {
		this.ti = ti;
		this.tx = tx;
		this.doLog = doLog;
		fileName = ti.fileName();
		headerBlk = new BlockId(fileName, 0);
	}

	/**
	 * Format the header of specified file.
	 * 
	 * @param fileName
	 *            the file name
	 * @param tx
	 *            the transaction
	 */
	public static void formatFileHeader(String fileName, Transaction tx) {
		try {
			tx.concurrencyMgr().modifyFile(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		// header should be the first block of a given file
		if (VanillaDb.fileMgr().size(fileName) == 0) {
			FileHeaderFormatter fhf = new FileHeaderFormatter();
			Buffer buff = VanillaDb.bufferMgr().pinNew(fileName, fhf,
					tx.getTransactionNumber());
			VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);
		}
	}

	/**
	 * Closes the record file.
	 */
	public void close() {
		if (rp != null)
			rp.close();
		if (fhp != null)
			closeHeader();
	}

	/**
	 * Positions the current record so that a call to method next will wind up
	 * at the first record.
	 */
	public void beforeFirst() {
		close();
		currentBlkNum = 0; // first data block is block 1
	}

	/**
	 * Moves to the next record. Returns false if there is no next record.
	 * 
	 * @return false if there is no next record.
	 */
	public boolean next() {
		if (currentBlkNum == 0 && !moveTo(1))
			return false;
		while (true) {
			if (rp.next())
				return true;
			if (!moveTo(currentBlkNum + 1))
				return false;
		}
	}

	/**
	 * Returns the value of the specified field in the current record. Getter
	 * should be called after {@link #next()} or {@link #moveToRecordId()}.
	 * 
	 * @param fldName
	 *            the name of the field
	 * 
	 * @return the value at that field
	 */
	public Constant getVal(String fldName) {
		return rp.getVal(fldName);
	}

	/**
	 * Sets a value of the specified field in the current record. The type of
	 * the value must be equal to that of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the new value for the field
	 */
	public void setVal(String fldName, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		Type fldType = ti.schema().type(fldName);

		Constant v = val.castTo(fldType);
		if (Page.size(v) > Page.maxSize(fldType))
			throw new SchemaIncompatibleException();
		rp.setVal(fldName, v);
	}

	/**
	 * Deletes the current record. The client must call next() to move to the
	 * next record. Calls to methods on a deleted record have unspecified
	 * behavior.
	 */
	public void delete() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();

		if (fhp == null)
			fhp = openHeaderForModification();
		rp.delete(fhp.getLastDeletedSlot());
		fhp.setLastDeletedSlot(currentRecordId());
		closeHeader();
	}

	/**
	 * Inserts a new, blank record somewhere in the file beginning at the
	 * current record. If the new record does not fit into an existing block,
	 * then a new block is appended to the file.
	 */
	public void insert() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		try {
			// insertion may change the properties of file
			if (!isTempTable())
				tx.concurrencyMgr().modifyFile(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		if (fhp == null)
			fhp = openHeaderForModification();
		if (fhp.hasDeletedSlots()) { // insert into deleted slot
			moveToRecordId(fhp.getLastDeletedSlot());
			RecordId lds = rp.insertIntoDeletedSlot();
			fhp.setLastDeletedSlot(lds);
		} else {
			if (!fhp.hasDataRecords()) { // no record inserted before
				appendBlock();
				moveTo(1);
				rp.insertIntoNextEmptySlot();
			} else {
				RecordId tailSlot = fhp.getTailSolt();
				moveToRecordId(tailSlot);
				while (!rp.insertIntoNextEmptySlot()) {
					if (atLastBlock())
						appendBlock();
					moveTo(currentBlkNum + 1);
				}
			}
			fhp.setTailSolt(currentRecordId());
		}
		closeHeader();
	}

	/**
	 * Positions the current record as indicated by the specified record ID .
	 * 
	 * @param rid
	 *            a record ID
	 */
	public void moveToRecordId(RecordId rid) {
		moveTo(rid.block().number());
		rp.moveToId(rid.id());
	}

	/**
	 * Returns the record ID of the current record.
	 * 
	 * @return a record ID
	 */
	public RecordId currentRecordId() {
		int id = rp.currentId();
		return new RecordId(new BlockId(fileName, currentBlkNum), id);
	}

	/**
	 * Returns the number of blocks in the specified file. This method first
	 * calls corresponding concurrency manager to guarantee the isolation
	 * property, before asking the file manager to return the file size.
	 * 
	 * @return the number of blocks in the file
	 */
	public long fileSize() {
		try {
			if (!isTempTable())
				tx.concurrencyMgr().readFile(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		return VanillaDb.fileMgr().size(fileName);
	}

	private boolean moveTo(long b) {
		if (rp != null)
			rp.close();

		if (b >= fileSize()) // block b not allocated yet
			return false;
		currentBlkNum = b;
		BlockId blk = new BlockId(fileName, currentBlkNum);
		rp = new RecordPage(blk, ti, tx, doLog);
		return true;
	}

	private void appendBlock() {
		try {
			if (!isTempTable())
				tx.concurrencyMgr().modifyFile(fileName);
			RecordFormatter fmtr = new RecordFormatter(ti);
			Buffer buff = VanillaDb.bufferMgr().pinNew(fileName, fmtr,
					tx.getTransactionNumber());
			VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);
			if (!isTempTable())
				tx.concurrencyMgr().insertBlock(buff.block());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}

	}

	private FileHeaderPage openHeaderForModification() {
		try {
			// acquires exclusive access to the header
			if (!isTempTable())
				tx.concurrencyMgr().lockRecordFileHeader(headerBlk);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		return new FileHeaderPage(fileName, tx);
	}

	private void closeHeader() {
		if (fhp != null) {
			tx.concurrencyMgr().releaseRecordFileHeader(headerBlk);
			fhp = null;
		}
	}

	private boolean isTempTable() {
		return fileName.startsWith("_temp");
	}

	private boolean atLastBlock() {
		return currentBlkNum == fileSize() - 1;
	}
}