package org.vanilladb.dd.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public class StoredProcRequestRecord implements DdLogRecord {
	private long txNum;
	private int cid, rteId, pid;
	private Object[] pars;

	/**
	 * 
	 * Creates a new stored procedure request log record for the specified
	 * transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param cid
	 * @param pid
	 * @param pars
	 */
	public StoredProcRequestRecord(long txNum, int cid, int rteId, int pid,
			Object... pars) {
		this.txNum = txNum;
		this.cid = cid;
		this.rteId = rteId;
		this.pid = pid;
		this.pars = pars;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public StoredProcRequestRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		this.cid = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.rteId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.pid = (Integer) rec.nextVal(INTEGER).asJavaVal();

		// FIXME
		// See writeToLog()
		this.pars = new Object[0];
	}

	/**
	 * Writes a request record to the log. This log record contains the
	 * {@link LogRecord#OP_REQUEST} operator ID, followed by the transaction ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		// TODO Auto-generated method stub
		Constant[] rec = new Constant[] { new BigIntConstant(this.txNum),
				new IntegerConstant(this.cid), new IntegerConstant(this.rteId),
				new IntegerConstant(this.pid),
				// XXX
				// How to write object array to log?
				new VarcharConstant(pars.toString()) };
		return ddLogMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_SP_REQUEST;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {
		// do nothing
	}

	@Override
	public void redo(Transaction tx) {
		// TODO replay the stored procedure
	}

	@Override
	public String toString() {
		return "<SP_REQUEST " + txNum + " " + pid + " " + cid + " >";
	}
}