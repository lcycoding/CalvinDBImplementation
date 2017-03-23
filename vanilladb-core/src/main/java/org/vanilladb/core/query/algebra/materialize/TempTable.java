package org.vanilladb.core.query.algebra.materialize;

import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.FileHeaderFormatter;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A class that creates temporary tables. A temporary table is not registered in
 * the catalog. The class therefore has a method getTableInfo to return the
 * table's metadata.
 */
public class TempTable {
	private static long nextTableNum = 0;
	private TableInfo ti;
	private Transaction tx;

	/**
	 * Allocates a name for for a new temporary table having the specified
	 * schema.
	 * 
	 * @param sch
	 *            the new table's schema
	 * @param tx
	 *            the calling transaction
	 */
	public TempTable(Schema sch, Transaction tx) {
		String tblname = nextTableName();
		ti = new TableInfo(tblname, sch);
		this.tx = tx;
		FileHeaderFormatter fhf = new FileHeaderFormatter();
		Buffer buff = VanillaDb.bufferMgr().pinNew(ti.fileName(), fhf,
				tx.getTransactionNumber());
		VanillaDb.bufferMgr().unpin(tx.getTransactionNumber(), buff);
	}

	/**
	 * Opens a table scan for the temporary table.
	 */
	public UpdateScan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Return the table's metadata.
	 * 
	 * @return the table's metadata
	 */
	public TableInfo getTableInfo() {
		return ti;
	}

	private static synchronized String nextTableName() {
		nextTableNum++;
		return "_temp" + nextTableNum;
	}

}