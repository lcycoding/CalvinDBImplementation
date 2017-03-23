package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.tx.Transaction;

class SetValueRecord implements LogRecord {
	private long txNum;
	private int offset;
	private Constant val;
	private Constant newVal;
	private BlockId blk;

	/**
	 * Creates a new setval log record.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param blk
	 *            the block containing the value
	 * @param offset
	 *            the offset of the value in the block
	 * @param val
	 *            the old value
	 * @param newVal
	 *            the new value
	 */
	public SetValueRecord(long txNum, BlockId blk, int offset, Constant val,
			Constant newVal) {
		this.txNum = txNum;
		this.blk = blk;
		this.offset = offset;
		this.val = val;
		this.newVal = newVal;
	}

	/**
	 * Creates a log record by reading five other values from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 * @param op
	 *            the operation ID
	 */
	public SetValueRecord(BasicLogRecord rec, int op) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		this.blk = new BlockId((String) rec.nextVal(VARCHAR).asJavaVal(),
				(Long) rec.nextVal(BIGINT).asJavaVal());
		this.offset = (Integer) rec.nextVal(INTEGER).asJavaVal();
		this.val = rec.nextVal(Type.newInstance(op));
		this.newVal = rec.nextVal(Type.newInstance(op));
	}

	/**
	 * Writes a setval record to the log. This log record contains the SQL type
	 * corresponding to the value as the operator ID, followed by the
	 * transaction ID, the filename, block number, and offset of the modified
	 * block, and the previous integer value at that offset.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(op()),
				new BigIntConstant(txNum), new VarcharConstant(blk.fileName()),
				new BigIntConstant(blk.number()), new IntegerConstant(offset),
				val, newVal };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return val.getType().getSqlType();
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public String toString() {
		return "<SETVAL " + op() + " " + txNum + " " + blk + " " + offset + " "
				+ val + ">";
	}

	/**
	 * Replaces the specified data value with the value saved in the log record.
	 * The method pins a buffer to the specified block, calls setInt to restore
	 * the saved value (using a dummy LSN), and unpins the buffer.
	 * 
	 * @see LogRecord#undo(Transaction)
	 */
	@Override
	public void undo(Transaction tx) {
		Buffer buff = VanillaDb.bufferMgr().pin(blk, txNumber());
		buff.setVal(offset, val, tx.getTransactionNumber(), -1);
		VanillaDb.bufferMgr().unpin(txNumber(), buff);
	}

	/**
	 * Replaces the specified data value with the new value saved in the log
	 * record. The method pins a buffer to the specified block, calls setInt to
	 * restore the saved value (using a dummy LSN), and unpins the buffer.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {
		Buffer buff = VanillaDb.bufferMgr().pin(blk, txNumber());
		buff.setVal(offset, newVal, tx.getTransactionNumber(), -1);
		VanillaDb.bufferMgr().unpin(txNumber(), buff);
	}
}
