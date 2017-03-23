package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

/**
 * A B-tree directory page that iterates over the B-tree directory blocks in a
 * file.
 * <p>
 * There is one flag in each B-tree directory block: the level (starting from 0
 * at the deepest) of that block in the directory.
 * </p>
 */
public class BTreeDir {
	/**
	 * A field name of the schema of B-tree directory records.
	 */
	static final String SCH_KEY = "key", SCH_CHILD = "child";

	static int NUM_FLAGS = 1;

	/**
	 * Returns the schema of the B-tree directory records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	static Schema schema(Type fldType) {
		Schema sch = new Schema();
		sch.addField(SCH_KEY, fldType);
		sch.addField(SCH_CHILD, BIGINT);
		return sch;
	}

	static long getLevelFlag(BTreePage p) {
		return p.getFlag(0);
	}

	static void setLevelFlag(BTreePage p, long val) {
		p.setFlag(0, val);
	}

	static Constant getKey(BTreePage p, int slot) {
		return p.getVal(slot, SCH_KEY);
	}

	static long getChildBlockNumber(BTreePage p, int slot) {
		return (Long) p.getVal(slot, SCH_CHILD).asJavaVal();
	}

	static void insert(BTreePage p, int slot, Constant val, long blkNum) {
		p.insert(slot);
		p.setVal(slot, SCH_KEY, val);
		p.setVal(slot, SCH_CHILD, new BigIntConstant(blkNum));
	}

	private TableInfo ti;
	private Transaction tx;
	private ConcurrencyMgr ccMgr;
	private BTreePage contents;
	private String dataFileName;

	private List<BlockId> dirsMayBeUpdated;

	/**
	 * Creates an object to hold the contents of the specified B-tree block.
	 * 
	 * @param fileName
	 *            the data file name
	 * @param blk
	 *            a block ID refers to the specified B-tree block
	 * @param ti
	 *            the metadata of the B-tree directory file
	 * @param tx
	 *            the calling transaction
	 */
	BTreeDir(String dataFileName, BlockId blk, TableInfo ti, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		this.dataFileName = dataFileName;
		ccMgr = tx.concurrencyMgr();
		contents = new BTreePage(dataFileName, blk, NUM_FLAGS, ti, tx);
	}

	/**
	 * Closes the directory page.
	 */
	public void close() {
		contents.close();
		dirsMayBeUpdated = null;
	}

	/**
	 * Returns the block number of the B-tree leaf block that contains the
	 * specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * 
	 * @param leafTi
	 *            the metadata of the B-tree leaf file
	 * @return the BlockId of the leaf block containing that search key
	 */
	public BlockId search(Constant searchKey, TableInfo leafTi, int purpose) {
		if (purpose == BTreeIndex.READ)
			return searchForRead(searchKey, leafTi);
		else if (purpose == BTreeIndex.INSERT)
			return searchForInsert(searchKey, leafTi);
		else if (purpose == BTreeIndex.DELETE)
			return searchForDelete(searchKey, leafTi);
		else
			throw new UnsupportedOperationException();
	}

	public List<BlockId> dirsMayBeUpdated() {
		return dirsMayBeUpdated;
	}

	/**
	 * Creates a new root block for the B-tree. The new root will have two
	 * children: the old root, and the specified block. Since the root must
	 * always be in block 0 of the file, the contents of block 0 will get
	 * transferred to a new block (serving as the old root).
	 * 
	 * @param e
	 *            the directory entry to be added as a child of the new root
	 */
	public void makeNewRoot(DirEntry e) {
		// check that the content is the root block
		if (contents.currentBlk().number() != 0) {
			contents.close();
			contents = new BTreePage(dataFileName,
					new BlockId(ti.fileName(), 0), NUM_FLAGS, ti, tx);
		}
		Constant firstval = getKey(contents, 0);
		long level = getLevelFlag(contents);
		// transfer all records to the new block
		long newBlkNum = contents.split(0, new long[] { level });
		DirEntry oldRootEntry = new DirEntry(firstval, newBlkNum);
		insertEntry(oldRootEntry);
		insertEntry(e);
		setLevelFlag(contents, level + 1);
	}

	/**
	 * Inserts a new directory entry into the B-tree directory block. If the
	 * block is at level 0, then the entry is inserted there. Otherwise, the
	 * entry is inserted into the appropriate child node, and the return value
	 * is examined. A non-null return value indicates that the child node
	 * splits, and so the returned entry is inserted into this block. If this
	 * block splits, then the method similarly returns the entry of the new
	 * block to its caller; otherwise, the method returns null.
	 * 
	 * @param e
	 *            the directory entry to be inserted
	 * @return the directory entry of the newly-split block, if one exists;
	 *         otherwise, null
	 */
	@Deprecated
	public DirEntry insert(DirEntry e) {
		try {
			ccMgr.crabDownDirBlockForModification(contents.currentBlk());
			if (getLevelFlag(contents) == 0)
				return insertEntry(e);

			BlockId childBlk = new BlockId(ti.fileName(),
					findChildBlockNumber(e.key()));
			ccMgr.crabDownDirBlockForModification(childBlk);
			BTreeDir child = new BTreeDir(dataFileName, childBlk, ti, tx);
			// crabs back the parent if the child is not possible to split
			if (!child.contents.isGettingFull())
				ccMgr.crabBackDirBlockForModification(contents.currentBlk());

			/*
			 * Recursive calls to the child's insert(). All the blocks in the
			 * calling stack will be pinned simultaneously.
			 */
			DirEntry myEntry = child.insert(e);
			child.close();
			if (myEntry == null) {
				// if the child does not split, releases unused exclusive lock
				ccMgr.crabBackDirBlockForModification(contents.currentBlk());
				return null;
			}
			return insertEntry(myEntry);
		} catch (LockAbortException le) {
			tx.rollback();
			throw le;
		}
	}

	public DirEntry insertEntry(DirEntry e) {
		int newslot = 1 + findSlotBefore(e.key());
		insert(contents, newslot, e.key(), e.blockNumber());
		if (!contents.isFull())
			return null;
		// split full page
		int splitPos = contents.getNumRecords() / 2;
		Constant splitVal = getKey(contents, splitPos);
		long newBlkNum = contents.split(splitPos,
				new long[] { getLevelFlag(contents) });
		return new DirEntry(splitVal, newBlkNum);
	}

	private BlockId searchForInsert(Constant searchKey, TableInfo leafTi) {
		// search from root to level 0
		dirsMayBeUpdated = new ArrayList<BlockId>();
		BlockId parentBlk = contents.currentBlk();
		try {
			ccMgr.crabDownDirBlockForModification(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;
			dirsMayBeUpdated.add(parentBlk);

			// if it's not the lowest directory block
			while (getLevelFlag(contents) > 0) {
				// read child block
				childBlk = new BlockId(ti.fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForModification(childBlk);
				BTreePage child = new BTreePage(dataFileName, childBlk,
						NUM_FLAGS, ti, tx);
				
				// crabs back the parent if the child is not possible to split
				if (!child.isGettingFull()) {
					for (int i = dirsMayBeUpdated.size() - 1; i >= 0; i--)
						ccMgr.crabBackDirBlockForModification(dirsMayBeUpdated
								.get(i));
					dirsMayBeUpdated.clear();
				}
				dirsMayBeUpdated.add(childBlk);
				
				// move current block to child block
				contents.close();
				contents = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = contents.currentBlk();
			}
			
			// get leaf block id
			BlockId leafBlk = new BlockId(leafTi.fileName(), childBlkNum);
			ccMgr.modifyLeafBlock(leafBlk); // exclusive lock
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private BlockId searchForDelete(Constant searchKey, TableInfo leafTi) {
		// search from root to level 0
		BlockId parentBlk = contents.currentBlk();
		try {
			ccMgr.crabDownDirBlockForRead(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;

			// if it's not the lowest directory block
			while (getLevelFlag(contents) > 0) {
				// read child block
				childBlk = new BlockId(ti.fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForRead(childBlk);
				BTreePage child = new BTreePage(dataFileName, childBlk,
						NUM_FLAGS, ti, tx);
				
				// release parent block
				ccMgr.crabBackDirBlockForRead(parentBlk);
				contents.close();
				
				// move current block to child block
				contents = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = contents.currentBlk();
			}
			
			// get leaf block id
			BlockId leafBlk = new BlockId(leafTi.fileName(), childBlkNum);
			ccMgr.modifyLeafBlock(leafBlk); // exclusive lock
			ccMgr.crabBackDirBlockForRead(contents.currentBlk());
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private BlockId searchForRead(Constant searchKey, TableInfo leafTi) {
		// search from root to level 0
		BlockId parentBlk = contents.currentBlk();
		try {
			ccMgr.crabDownDirBlockForRead(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;
			
			// if it's not the lowest directory block
			while (getLevelFlag(contents) > 0) {
				// read child block
				childBlk = new BlockId(ti.fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForRead(childBlk);
				BTreePage child = new BTreePage(dataFileName, childBlk,
						NUM_FLAGS, ti, tx);
				
				// release parent block
				ccMgr.crabBackDirBlockForRead(parentBlk);
				contents.close();
				
				// move current block to child block
				contents = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = contents.currentBlk();
			}
			
			// get leaf block id
			BlockId leafBlk = new BlockId(leafTi.fileName(), childBlkNum);
			ccMgr.readLeafBlock(leafBlk); // shared lock
			ccMgr.crabBackDirBlockForRead(contents.currentBlk());
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private long findChildBlockNumber(Constant searchKey) {
		int slot = findSlotBefore(searchKey);
		if (getKey(contents, slot + 1).equals(searchKey))
			slot++;
		return getChildBlockNumber(contents, slot);
	}

	/**
	 * Calculates the slot right before the one having the specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * @return the position before where the search key goes
	 */
	private int findSlotBefore(Constant searchKey) {
		/*
		int slot = 0;
		while (slot < contents.getNumRecords()
				&& getKey(contents, slot).compareTo(searchKey) < 0)
			slot++;
		return slot - 1;
		*/
		// Optimization: Use binary search rather than sequential search
		int startSlot = 0, endSlot = contents.getNumRecords() - 1;
		int middleSlot = (startSlot + endSlot) / 2;
		
		if (endSlot >= 0) {
			while (middleSlot != startSlot) {
				if (getKey(contents, middleSlot).compareTo(searchKey) < 0)
					startSlot = middleSlot;
				else
					endSlot = middleSlot;
				
				middleSlot = (startSlot + endSlot) / 2;
			}
			
			if (getKey(contents, endSlot).compareTo(searchKey) < 0)
				return endSlot;
			else if (getKey(contents, startSlot).compareTo(searchKey) < 0)
				return startSlot;
			else
				return startSlot - 1;
		} else
			return -1;
	}
}
