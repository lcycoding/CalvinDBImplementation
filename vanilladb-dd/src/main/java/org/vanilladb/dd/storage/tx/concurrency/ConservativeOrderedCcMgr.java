package org.vanilladb.dd.storage.tx.concurrency;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.dd.sql.RecordKey;

public class ConservativeOrderedCcMgr extends ConcurrencyMgr {
	protected static ConservativeOrderedLockTable lockTbl = new ConservativeOrderedLockTable();
	
	public ConservativeOrderedCcMgr(long txNumber) {
		txNum = txNumber;
	}

	public void prepareSp(String[] readTables, String[] writeTables) {
		if (readTables != null)
			for (String rt : readTables)
				lockTbl.requestLock(rt, txNum);

		if (writeTables != null)
			for (String wt : writeTables)
				lockTbl.requestLock(wt, txNum);
	}

	public void executeSp(String[] readTables, String[] writeTables) {
		if (writeTables != null)
			for (String s : writeTables)
				lockTbl.xLock(s, txNum);

		if (readTables != null)
			for (String s : readTables)
				lockTbl.sLock(s, txNum);
	}

	public void prepareSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		if (readKeys != null)
			for (RecordKey rt : readKeys)
				lockTbl.requestLock(rt, txNum);

		if (writeKeys != null)
			for (RecordKey wt : writeKeys)
				lockTbl.requestLock(wt, txNum);
	}

	public void executeSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		/*
		 * TODO: should take intension lock on tables? If the structure of
		 * record file may change, the ix lock on table level is needed.
		 */
		if (writeKeys != null)
			for (RecordKey k : writeKeys) {
				// lockTbl.ixLock(k.getTableName(), txNum);
				lockTbl.xLock(k, txNum);
			}

		if (readKeys != null)
			for (RecordKey k : readKeys) {
				// lockTbl.isLock(k.getTableName(), txNum);
				lockTbl.sLock(k, txNum);
			}
	}

	public void finishSp(RecordKey[] readKeys, RecordKey[] writeKeys) {
		if (writeKeys != null)
			for (RecordKey k : writeKeys) {
				// TODO: release table ixlock
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.X_LOCK);
			}

		if (readKeys != null)
			for (RecordKey k : readKeys) {
				// TODO: release table islock
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.S_LOCK);
			}
	}

	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum);
	}

	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum);
	}

	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	public void prepareWriteBack(RecordKey... keys) {
		lockTbl.requestWriteBackLocks(keys, txNum);
	}

	public void executeWriteBack(RecordKey... keys) {
		if (keys != null)
			for (RecordKey k : keys)
				lockTbl.wbLock(k, txNum);
	}

	public void releaseWriteBackLock(RecordKey... keys) {
		if (keys != null)
			for (RecordKey k : keys)
				lockTbl.release(k, txNum,
						ConservativeOrderedLockTable.LockType.WRITE_BACK_LOCK);
	}

	@Override
	public void modifyFile(String fileName) {
		// do nothing
	}

	@Override
	public void readFile(String fileName) {
		// do nothing
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void readBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void insertBlock(BlockId blk) {
		// do nothing
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// lockTbl.ixLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		// lockTbl.isLock(dataFileName, txNum);
	}

	/*
	 * Methods for B-Tree index locking
	 */
	private Set<BlockId> readIndexBlks = new HashSet<BlockId>();
	private Set<BlockId> writtenIndexBlks = new HashSet<BlockId>();

	/**
	 * Sets lock on the leaf block for update.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void modifyLeafBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
		writtenIndexBlks.add(blk);
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
		writtenIndexBlks.add(blk);
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
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.X_LOCK);
		writtenIndexBlks.remove(blk);
	}

	/**
	 * Releases shared locks on the directory block for crabbing back.
	 * 
	 * @param blk
	 *            the block id
	 */
	public void crabBackDirBlockForRead(BlockId blk) {
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.S_LOCK);
		readIndexBlks.remove(blk);
	}

	public void releaseIndexLocks() {
		for (BlockId blk : readIndexBlks)
			lockTbl.release(blk, txNum,
					ConservativeOrderedLockTable.LockType.S_LOCK);
		for (BlockId blk : writtenIndexBlks)
			lockTbl.release(blk, txNum,
					ConservativeOrderedLockTable.LockType.X_LOCK);
		readIndexBlks.clear();
		writtenIndexBlks.clear();
	}

	public void lockRecordFileHeader(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	public void releaseRecordFileHeader(BlockId blk) {
		lockTbl.release(blk, txNum,
				ConservativeOrderedLockTable.LockType.X_LOCK);
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// do nothing

	}

	@Override
	public void readRecord(RecordId recId) {
		// do nothing

	}
}