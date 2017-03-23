package org.vanilladb.core.storage.metadata;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The metadata about a table and its records.
 */
public class TableInfo {
	private Schema schema;
	private String tblName;

	/**
	 * Creates a TableInfo object, given a table name and schema. The
	 * constructor calculates the physical offset of each field. This
	 * constructor is used when a table is created.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param schema
	 *            the schema of the table's records
	 */
	public TableInfo(String tblName, Schema schema) {
		this.schema = schema;
		this.tblName = tblName;
	}

	/**
	 * Returns the filename assigned to this table. Currently, the filename is
	 * the table name followed by ".tbl".
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String fileName() {
		return tblName + ".tbl";
	}

	/**
	 * Returns the table name of this TableInfo
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the schema of the table's records
	 * 
	 * @return the table's record schema
	 */
	public Schema schema() {
		return schema;
	}

	/**
	 * Opens the {@link RecordFile} described by this object.
	 * 
	 * @return the {@link RecordFile} object associated with this information
	 */
	public RecordFile open(Transaction tx, boolean doLog) {
		return new RecordFile(this, tx, doLog);
	}
}