package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

/**
 * A B-tree implementation of {@link Index}.
 */
public class BTreeIndex extends Index {
	protected static final int READ = 1, INSERT = 2, DELETE = 3;

	private IndexInfo ii;
	private Transaction tx;
	private ConcurrencyMgr ccMgr;
	private TableInfo dirTi, leafTi;
	private BTreeLeaf leaf = null;
	private BlockId rootBlk;
	private String dataFileName;
	private Type dataType;

	private List<BlockId> dirsMayBeUpdated;

	public static long searchCost(Type fldType, long totRecs, long matchRecs) {
		int dirRpb = BLOCK_SIZE / BTreePage.slotSize(BTreeDir.schema(fldType));
		int leafRpb = BLOCK_SIZE
				/ BTreePage.slotSize(BTreeLeaf.schema(fldType));
		long leafs = (int) Math.ceil((double) totRecs / leafRpb);
		long matchLeafs = (int) Math.ceil((double) matchRecs / leafRpb);
		return (long) Math.ceil(Math.log(leafs) / Math.log(dirRpb))
				+ matchLeafs;
	}

	/**
	 * Opens a B-tree index for the specified index. The method determines the
	 * appropriate files for the leaf and directory records, creating them if
	 * they did not exist.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param fldType
	 *            the type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public BTreeIndex(IndexInfo ii, Type fldType, Transaction tx) {
		this.ii = ii;
		this.dataFileName = ii.tableName() + ".tbl";
		this.tx = tx;
		ccMgr = tx.concurrencyMgr();
		dataType = fldType;

		// deal with the leaves
		leafTi = new TableInfo(ii.indexName() + "leaf",
				BTreeLeaf.schema(fldType));

		// initialize the first block in leaf
		if (fileSize(leafTi.fileName()) == 0)
			appendBlock(leafTi, new long[] { -1, -1 });

		// deal with the directory
		dirTi = new TableInfo(ii.indexName() + "dir", BTreeDir.schema(fldType));
		rootBlk = new BlockId(dirTi.fileName(), 0);

		if (fileSize(dirTi.fileName()) == 0)
			appendBlock(dirTi, new long[] { 0 });

		BTreePage rootpage = new BTreePage(dataFileName, rootBlk,
				BTreeDir.NUM_FLAGS, dirTi, tx);
		if (rootpage.getNumRecords() == 0) {
			// insert initial directory entry
			Constant minval = dataType.minValue();
			BTreeDir.insert(rootpage, 0, minval, 0);
		}
		rootpage.close();
	}

	@Override
	public void preLoadToMemory() {
		
		// Read all blocks of the directory file
		String dirName = dirTi.fileName();
		long dirSize = fileSize(dirName);
		BlockId blk;
		for (int i = 0; i < dirSize; i++) {
			blk = new BlockId(dirName, i);
			VanillaDb.bufferMgr().pin(blk, tx.getTransactionNumber());
		}
		
		// Read all blocks of the leaf file
		String leafName = leafTi.fileName();
		long leafSize = fileSize(leafName);
		for (int i = 0; i < leafSize; i++) {
			blk = new BlockId(leafName, i);
			VanillaDb.bufferMgr().pin(blk, tx.getTransactionNumber());
		}
	}

	/**
	 * Traverses the directory to find the leaf page corresponding to the lower
	 * bound of the specified key range. The method then position the page
	 * before the first record (if any) matching the that lower bound. The leaf
	 * page is kept open, for use by the methods {@link #next} and
	 * {@link #getDataRecordId}.
	 * 
	 * @see Index#beforeFirst
	 */
	@Override
	public void beforeFirst(ConstantRange searchRange) {
		if (!searchRange.isValid())
			return;
		
		search(searchRange, READ);
	}

	/**
	 * Moves to the next index record in B-tree leaves matching the
	 * previously-specified search key. Returns false if there are no more such
	 * records.
	 * 
	 * @see Index#next
	 */
	@Override
	public boolean next() {
		return leaf == null ? false : leaf.next();
	}

	/**
	 * Returns the data record ID from the current index record in B-tree
	 * leaves.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		return leaf.getDataRecordId();
	}

	/**
	 * Inserts the specified record into the index. The method first traverses
	 * the directory to find the appropriate leaf page; then it inserts the
	 * record into the leaf. If the insertion causes the leaf to split, then the
	 * method calls insert on the root, passing it the directory entry of the
	 * new leaf page. If the root node splits, then {@link BTreeDir#makeNewRoot}
	 * is called.
	 * 
	 * @see Index#insert(Constant, RecordId)
	 */
	@Override
	public void insert(Constant key, RecordId dataRecordId) {
		if (tx.isReadOnly())
			throw new UnsupportedOperationException();

		// performs index logical log
		tx.recoveryMgr().logIndexInsertion(ii.tableName(), ii.fieldName(), key,
				dataRecordId);
		
		// search leaf block for insertion
		search(ConstantRange.newInstance(key), INSERT);
		DirEntry newEntry = leaf.insert(dataRecordId);
		leaf.close();
		if (newEntry == null)
			return;

		// insert the directory entry from the lowest directory
		for (int i = dirsMayBeUpdated.size() - 1; i >= 0; i--) {
			BlockId dirBlk = dirsMayBeUpdated.get(i);
			BTreeDir dir = new BTreeDir(dataFileName, dirBlk, dirTi, tx);
			newEntry = dir.insertEntry(newEntry);
			dir.close();
			if (newEntry == null)
				break;
		}
		if (newEntry != null) {
			BTreeDir root = new BTreeDir(dataFileName, rootBlk, dirTi, tx);
			root.makeNewRoot(newEntry);
			root.close();
		}
		dirsMayBeUpdated = null;
		close();
	}

	/**
	 * Deletes the specified index record. The method first traverses the
	 * directory to find the leaf page containing that record; then it deletes
	 * the record from the page. F
	 * 
	 * @see Index#delete(Constant, RecordId)
	 */
	@Override
	public void delete(Constant key, RecordId dataRecordId) {
		if (tx.isReadOnly())
			throw new UnsupportedOperationException();

		// performs index logical log
		tx.recoveryMgr().logIndexDeletion(ii.tableName(), ii.fieldName(), key,
				dataRecordId);

		search(ConstantRange.newInstance(key), DELETE);
		leaf.delete(dataRecordId);
		close();
	}

	/**
	 * Closes the index by closing its open leaf page, if necessary.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (leaf != null) {
			leaf.close();
			leaf = null;
		}
		// release all locks on index structure
		ccMgr.releaseIndexLocks();
		dirsMayBeUpdated = null;
	}

	private void search(ConstantRange searchRange, int purpose) {
		close();
		BlockId leafblk;
		BTreeDir root = new BTreeDir(dataFileName, rootBlk, dirTi, tx);
		if (!searchRange.hasLowerBound())
			leafblk = root.search(dataType.minValue(), leafTi, purpose);
		else
			leafblk = root.search(searchRange.low(), leafTi, purpose);
		
		// get the dir list for update
		if (purpose == INSERT)
			dirsMayBeUpdated = root.dirsMayBeUpdated();
		root.close();
		
		// read leaf block
		leaf = new BTreeLeaf(dataFileName, leafblk, leafTi, searchRange, tx);
	}

	private long fileSize(String fileName) {
		try {
			ccMgr.readFile(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		return VanillaDb.fileMgr().size(fileName);
	}

	private BlockId appendBlock(TableInfo ti, long[] flags) {
		try {
			ccMgr.modifyFile(ti.fileName());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		BTPageFormatter btpf = new BTPageFormatter(ti, flags);
		Buffer buff = VanillaDb.bufferMgr().pinNew(ti.fileName(), btpf,
				tx.getTransactionNumber());
		VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);
		return buff.block();
	}
}