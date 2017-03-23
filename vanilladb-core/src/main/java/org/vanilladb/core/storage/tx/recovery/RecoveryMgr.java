package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_CHECKPOINT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_COMMIT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_ROLLBACK;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_START;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;

/**
 * The recovery manager. Each transaction has its own recovery manager.
 */
public class RecoveryMgr implements TransactionLifecycleListener {
	private static boolean logSetVal = true;

	private long txNum; // the owner id of this recovery manger

	/**
	 * Creates a recovery manager for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public RecoveryMgr(long txNum, boolean isReadOnly) {
		this.txNum = txNum;
		if (!isReadOnly)
			new StartRecord(txNum).writeToLog();
	}

	public static void logSetVal(boolean log) {
		logSetVal = log;
	}

	/**
	 * Writes a commit record to the log, and then flushes the log record to
	 * disk.
	 */
	@Override
	public void onTxCommit(Transaction tx) {
		if (!tx.isReadOnly()) {
			long lsn = new CommitRecord(txNum).writeToLog();
			VanillaDb.logMgr().flush(lsn);
		}
	}

	/**
	 * Does the roll back process, writes a rollback record to the log, and
	 * flushes the log record to disk.
	 */
	@Override
	public void onTxRollback(Transaction tx) {
		if (!tx.isReadOnly()) {
			doRollback(tx);
			long lsn = new RollbackRecord(txNum).writeToLog();
			VanillaDb.logMgr().flush(lsn);
		}
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	/**
	 * 
	 * Goes through the log, rolling back all uncompleted transactions. Flushes
	 * all modified blocks. Finally, writes a quiescent checkpoint record to the
	 * log and flush it. This method should be called only during system
	 * startup, before user transactions begin.
	 */
	public static void recover(Transaction tx) {
		doRecover(tx);
		VanillaDb.bufferMgr().flushAll(tx.getTransactionNumber());
		long lsn = new CheckpointRecord().writeToLog();
		VanillaDb.logMgr().flush(lsn);
	}

	/**
	 * Writes a checkpoint record to the log.
	 * 
	 * @param txNums
	 *            the transactions that are being executed when writing the
	 *            checkpoint.
	 * @return the LSN of the log record.
	 */
	public long checkpoint(List<Long> txNums) {
		return new CheckpointRecord(txNums).writeToLog();
	}

	/**
	 * Writes a set value record to the log.
	 * 
	 * @param buff
	 *            the buffer containing the page
	 * @param offset
	 *            the offset of the value in the page
	 * @param newVal
	 *            the value to be written
	 * @return the LSN of the log record, or -1 if updates to temporary files
	 */
	public long logSetVal(Buffer buff, int offset, Constant newVal) {
		if (logSetVal) {
			BlockId blk = buff.block();
			if (isTempBlock(blk))
				return -1;
			return new SetValueRecord(txNum, blk, offset, buff.getVal(offset,
					newVal.getType()), newVal).writeToLog();
		} else
			return -1;
	}

	/**
	 * Writes a index insertion record into the log.
	 * 
	 * @param dataTblName
	 *            the indexed data table name
	 * @param indexFldName
	 *            the indexed field name
	 * @param keyVal
	 *            the value of search key
	 * @param rid
	 *            the record id of inserted entry
	 * @return the LSN of the log record, or -1 if recovery manager turns off
	 *         the logging
	 */
	public long logIndexInsertion(String dataTblName, String fldName,
			Constant keyVal, RecordId rid) {
		if (logSetVal) {
			return new IndexInsertRecord(txNum, dataTblName, fldName, keyVal,
					rid).writeToLog();
		} else
			return -1;
	}

	/**
	 * Writes a index deletion record into the log.
	 * 
	 * @param dataTblName
	 *            the indexed data table name
	 * @param indexFldName
	 *            the indexed field name
	 * @param keyVal
	 *            the value of search key
	 * @param rid
	 *            the record id of inserted entry
	 * @return the LSN of the log record, or -1 if recovery manager turns off
	 *         the logging
	 */
	public long logIndexDeletion(String dataTblName, String fldName,
			Constant keyVal, RecordId rid) {
		if (logSetVal) {
			return new IndexDeleteRecord(txNum, dataTblName, fldName, keyVal,
					rid).writeToLog();
		} else
			return -1;
	}

	/**
	 * Rolls back the transaction. The method iterates through the log records,
	 * calling {@link LogRecord#undo(Transaction)} for each log record it finds
	 * for the transaction, until it finds the transaction's START record.
	 */
	private void doRollback(Transaction tx) {
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.txNumber() == txNum) {
				if (rec.op() == OP_START)
					return;
				rec.undo(tx);
			}
		}
	}

	/**
	 * Does a complete database recovery. The method iterates through the log
	 * records. Whenever it finds a log record for an unfinished transaction, it
	 * calls {@link LogRecord#undo(Transaction)} on that record. The method
	 * stops iterating forward when it encounters a CHECKPOINT record and finds
	 * all the transactions which were executing when the checkpoint took place,
	 * or when the end of the log is reached. The method then iterates backward
	 * and redoes all finished transactions. TODO fix comments...
	 */
	private static void doRecover(Transaction tx) {
		Set<Long> finishedTxs = new HashSet<Long>();
		Set<Long> committedTxs = new HashSet<Long>();
		Set<Long> unCompletedTxs = new HashSet<Long>();
		List<Long> txsOnCheckpointing = null;
		ReversibleIterator<LogRecord> iter = new LogRecordIterator();

		// analyze phase
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			int op = rec.op();
			if (op == OP_CHECKPOINT) {
				txsOnCheckpointing = ((CheckpointRecord) rec).activeTxNums();
				if (txsOnCheckpointing.size() == 0)
					break;
			}

			if (op == OP_COMMIT) {
				committedTxs.add(rec.txNumber());
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_ROLLBACK) {
				finishedTxs.add(rec.txNumber());
			} else if (op == OP_START && !finishedTxs.contains(rec.txNumber())) {
				unCompletedTxs.add(rec.txNumber());
			}

			if (txsOnCheckpointing != null && op == OP_START
					&& txsOnCheckpointing.contains(rec.txNumber()))
				txsOnCheckpointing.remove(rec.txNumber());
			// stop iterate forward if all start records of active tx have found
			if (txsOnCheckpointing != null && txsOnCheckpointing.size() == 0)
				break;
		}
		finishedTxs = null;
		/*
		 * redo phase: redo the actions performed by committed txs
		 */
		while (iter.hasPrevious()) {
			LogRecord rec = iter.previous();
			if (committedTxs.contains(rec.txNumber()))
				rec.redo(tx);
		}

		// remove the recovery tx from unCompletedTxs set
		unCompletedTxs.remove(tx.getTransactionNumber());

		iter = new LogRecordIterator();
		/*
		 * undo phase: undo all actions performed by the active txs during last
		 * crash
		 */
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			int op = rec.op();
			if (!unCompletedTxs.contains(rec.txNumber()) || op == OP_COMMIT
					|| op == OP_ROLLBACK)
				continue;

			if (op == OP_START)
				unCompletedTxs.remove(rec.txNumber());
			else
				rec.undo(tx);

			if (unCompletedTxs.size() == 0)
				break;
		}
	}

	/**
	 * Determines whether a block comes from a temporary file or not.
	 */
	private boolean isTempBlock(BlockId blk) {
		return blk.fileName().startsWith("_temp");
	}
}
