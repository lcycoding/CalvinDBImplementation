package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexInsertRecord implements LogRecord {
	private long txNum;
	private String dataTblName, indexFldName;
	private Constant keyVal;
	private RecordId rid;

	/**
	 * Creates a new index insertion log record.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 * @param dataTblName
	 *            the indexed data table name
	 * @param indexFldName
	 *            the indexed field name
	 * @param keyVal
	 *            the value of search key
	 * @param rid
	 *            the record id of inserted entry
	 */
	public IndexInsertRecord(long txNum, String dataTblName,
			String indexFldName, Constant keyVal, RecordId rid) {
		this.txNum = txNum;
		this.dataTblName = dataTblName;
		this.indexFldName = indexFldName;
		this.keyVal = keyVal;
		this.rid = rid;
	}

	/**
	 * Creates a log record by reading four other values from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public IndexInsertRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		dataTblName = (String) rec.nextVal(VARCHAR).asJavaVal();
		indexFldName = (String) rec.nextVal(VARCHAR).asJavaVal();
		int keyType = (Integer) rec.nextVal(INTEGER).asJavaVal();
		keyVal = rec.nextVal(Type.newInstance(keyType));
		BlockId blk = new BlockId(dataTblName + ".tbl", (Long) rec.nextVal(
				BIGINT).asJavaVal());
		rid = new RecordId(blk, (Integer) rec.nextVal(INTEGER).asJavaVal());
	}

	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(OP_INDEX_INSERT),
				new BigIntConstant(txNum), new VarcharConstant(dataTblName),
				new VarcharConstant(indexFldName),
				new IntegerConstant(keyVal.getType().getSqlType()), keyVal,
				new BigIntConstant(rid.block().number()),
				new IntegerConstant(rid.id()) };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_INDEX_INSERT;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public String toString() {
		return "<INDEX_INSERT " + txNum + " " + dataTblName + " "
				+ indexFldName + " " + keyVal + " " + rid + ">";
	}

	/**
	 * Deletes the specified key value with record id saved in the log record
	 * from the index.
	 * 
	 * @see LogRecord#undo(Transaction)
	 */
	@Override
	public void undo(Transaction tx) {
		IndexRecoveryUtil.deleteFromIndex(dataTblName, indexFldName, keyVal,
				rid, tx, txNum);
	}

	/**
	 * Re-inserts the specified key value with record id saved in the log record
	 * into the index.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {
		IndexRecoveryUtil.insertIntoIndex(dataTblName, indexFldName, keyVal,
				rid, tx, txNum);
	}
}