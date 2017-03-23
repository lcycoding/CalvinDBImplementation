package org.vanilladb.core.storage.tx.concurrency;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;

/**
 * A locking-based concurrency manager that controls when a {@link Transaction}
 * instance should be stalled to allow concurrency execution of multiple
 * transactions. Each transaction will have its own concurrency manager. This
 * class is intended to be extended to provide different isolation levels.
 */
public abstract class ConcurrencyMgr implements TransactionLifecycleListener {
	protected long txNum;

	protected static LockTable lockTbl = new LockTable();

	/**
	 * Sets lock according to the transaction's isolation level on the specified
	 * file for changing its properties.
	 * 
	 * @param fileName
	 *            the name of the file
	 */
	public abstract void modifyFile(String fileName);

	/**
	 * Sets lock according to the transaction's isolation level for reading the
	 * file properties or underlying records.
	 * 
	 * @param fileName
	 *            the name of the file
	 */
	public abstract void readFile(String fileName);

	/**
	 * Sets lock according to the transaction's isolation level for inserting
	 * this new block into the file.
	 * 
	 * @param blk
	 *            the block id
	 */
	public abstract void insertBlock(BlockId blk);

	/**
	 * Sets lock according to the transaction's isolation level for the updating
	 * some records in the specified block.
	 * 
	 * @param blk
	 *            the block id
	 */
	public abstract void modifyBlock(BlockId blk);

	/**
	 * Sets lock according to the transaction's isolation level for the reading
	 * some records in the specified block.
	 * 
	 * @param blk
	 *            the block id
	 */
	public abstract void readBlock(BlockId blk);
	
	/**
	 * Sets lock according to the transaction's isolation level for the updating
	 * specified record.
	 * 
	 * @param recId
	 *            the record id
	 */
	public abstract void modifyRecord(RecordId recId);

	/**
	 * Sets lock according to the transaction's isolation level for the reading
	 * specified record.
	 * 
	 * @param recId
	 *            the record id
	 */
	public abstract void readRecord(RecordId recId);

	/*
	 * Methods for B-Tree index locking
	 */
	private List<BlockId> readIndexBlks = new ArrayList<BlockId>();
	private List<BlockId> writenIndexBlks = new ArrayList<BlockId>();

	/**
	 * Sets lock on the data file for modifying its index.
	 * 
	 * @param dataFileName
	 *            the name of the data file
	 */
	public abstract void modifyIndex(String dataFileName);

	/**
	 * Sets lock on the data file for reading its index.
	 * 
	 * @param dataFileName
	 *            the name of the data file
	 */
	public abstract void readIndex(String dataFileName);

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writenIndexBlks.add(blk);
	}

	/**
	 * Sets lock on the leaf block for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void readLeafBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Sets exclusive lock on the directory block when crabbing down for
	 * modification.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForModification(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writenIndexBlks.add(blk);
	}

	/**
	 * Sets shared lock on the directory block when crabbing down for read.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabDownDirBlockForRead(BlockId blk) {
		lockTbl.sLock(blk, txNum);
		readIndexBlks.add(blk);
	}

	/**
	 * Releases exclusive locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForModification(BlockId blk) {
		lockTbl.release(blk, txNum, LockTable.X_LOCK);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum, LockTable.S_LOCK);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.release(blk, txNum, LockTable.S_LOCK);
		for (BlockId blk : writenIndexBlks)
			lockTbl.release(blk, txNum, LockTable.X_LOCK);
		readIndexBlks.clear();
		writenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum, LockTable.X_LOCK);
	}
}