package org.vanilladb.core.storage.tx.concurrency;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class ReadCommittedConcurrencyMgr extends ConcurrencyMgr {

	public ReadCommittedConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}

	@Override
	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	/**
	 * Releases all slocks obtained so far.
	 */
	@Override
	public void onTxEndStatement(Transaction tx) {
		lockTbl.releaseAll(txNum, true);
	}

	@Override
	public void modifyFile(String fileName) {
		lockTbl.xLock(fileName, txNum);
	}

	@Override
	public void readFile(String fileName) {
		lockTbl.isLock(fileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(fileName, txNum, LockTable.IS_LOCK);
	}

	@Override
	public void insertBlock(BlockId blk) {
		lockTbl.xLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void modifyBlock(BlockId blk) {
		lockTbl.ixLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void readBlock(BlockId blk) {
		lockTbl.isLock(blk.fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(blk.fileName(), txNum, LockTable.IS_LOCK);
		
		lockTbl.sLock(blk, txNum);
		// releases S lock to allow unrepeatable Read
		lockTbl.release(blk, txNum, LockTable.S_LOCK);
	}
	
	@Override
	public void modifyRecord(RecordId recId) {
		lockTbl.ixLock(recId.block().fileName(), txNum);
		lockTbl.ixLock(recId.block(), txNum);
		lockTbl.xLock(recId, txNum);
	}

	@Override
	public void readRecord(RecordId recId) {
		lockTbl.isLock(recId.block().fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block().fileName(), txNum, LockTable.IS_LOCK);
		
		lockTbl.isLock(recId.block(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block(), txNum, LockTable.IS_LOCK);
		
		lockTbl.sLock(recId, txNum);
		// releases S lock to allow unrepeatable Read
		lockTbl.release(recId, txNum, LockTable.S_LOCK);
	}

	@Override
	public void modifyIndex(String dataFileName) {
		lockTbl.xLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		lockTbl.isLock(dataFileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(dataFileName, txNum, LockTable.IS_LOCK);
	}
}