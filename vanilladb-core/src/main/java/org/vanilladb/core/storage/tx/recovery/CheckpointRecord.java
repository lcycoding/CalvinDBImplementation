package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The checkpoint log record.
 */
class CheckpointRecord implements LogRecord {
	private List<Long> txNums;

	/**
	 * Creates a quiescent checkpoint record.
	 */
	public CheckpointRecord() {
		this.txNums = new ArrayList<Long>();
	}

	/**
	 * Creates a non-quiescent checkpoint record.
	 */
	public CheckpointRecord(List<Long> txNums) {
		this.txNums = txNums;
	}

	/**
	 * Creates a log record by reading no other values from the basic log
	 * record.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public CheckpointRecord(BasicLogRecord rec) {
		int txCount = (Integer) rec.nextVal(INTEGER).asJavaVal();

		this.txNums = new ArrayList<Long>();
		for (int i = 0; i < txCount; i++) {
			txNums.add((Long) rec.nextVal(BIGINT).asJavaVal());
		}
	}

	/**
	 * Writes a checkpoint record to the log. This log record contains the
	 * {@link LogRecord#OP_CHECKPOINT} operator ID, number of active transctions
	 * during checkpointing and a list of active transaction ids.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		int recLength = txNums.size() + 2;
		Constant[] rec = new Constant[recLength];
		rec[0] = new IntegerConstant(OP_CHECKPOINT);
		rec[1] = new IntegerConstant(txNums.size());
		for (int i = 2; i < recLength; i++)
			rec[i] = new BigIntConstant(txNums.get(i - 2));
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_CHECKPOINT;
	}

	/**
	 * Checkpoint records have no associated transaction, and so the method
	 * returns a "dummy", negative txid.
	 */
	@Override
	public long txNumber() {
		return -1; // dummy value
	}

	/**
	 * Does nothing, because a checkpoint record contains no undo information.
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	/**
	 * Does nothing, because a checkpoint record contains no redo information.
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing
	}

	@Override
	public String toString() {
		StringBuffer strbuf = new StringBuffer("<NQCKPT");
		
		for (Long l : txNums) {
			strbuf.append(l + ",");
		}
		
		if (txNums.size() > 0)
			strbuf.delete(txNums.size() - 2, txNums.size() - 1);
		
		return strbuf.toString() + ">";
	}

	public List<Long> activeTxNums() {
		return this.txNums;
	}

	public boolean isContainTxNum(long txNum) {
		return this.txNums.contains(txNum);
	}
}
