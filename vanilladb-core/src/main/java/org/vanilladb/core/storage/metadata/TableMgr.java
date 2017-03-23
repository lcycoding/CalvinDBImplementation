package org.vanilladb.core.storage.metadata;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * The table manager. There are methods to create a table, save the metadata in
 * the catalog, and obtain the metadata of a previously-created table.
 */
public class TableMgr {
	/**
	 * Name of the table catalog.
	 */
	public static final String TCAT = "tblcat";

	/**
	 * A field name of the table catalog.
	 */
	public static final String TCAT_TBLNAME = "tblname";

	/**
	 * Name of the field catalog.
	 */
	public static final String FCAT = "fldcat";

	/**
	 * A field name of the field catalog.
	 */
	public static final String FCAT_TBLNAME = "tblname",
			FCAT_FLDNAME = "fldname", FCAT_TYPE = "type",
			FCAT_TYPEARG = "typearg";

	/**
	 * The maximum number of characters in any tablename or fieldname.
	 * Currently, this value is 30.
	 */
	public static final int MAX_NAME;

	private TableInfo tcatInfo, fcatInfo;
	// Optimization: Materialize the table information
	private Map<String, TableInfo> tiMap;

	static {
		MAX_NAME = CoreProperties.getLoader().getPropertyAsInteger(
				TableMgr.class.getName() + ".MAX_NAME", 30);
	}

	/**
	 * Creates a new catalog manager for the database system. If the database is
	 * new, then the two catalog tables are created.
	 * 
	 * @param isNew
	 *            has the value true if the database is new
	 * @param tx
	 *            the startup transaction
	 */
	public TableMgr(boolean isNew, Transaction tx) {
		tiMap = new HashMap<String, TableInfo>();
		Schema tcatSchema = new Schema();
		tcatSchema.addField(TCAT_TBLNAME, VARCHAR(MAX_NAME));
		tcatInfo = new TableInfo(TCAT, tcatSchema);

		Schema fcatSchema = new Schema();
		fcatSchema.addField(FCAT_TBLNAME, VARCHAR(MAX_NAME));
		fcatSchema.addField(FCAT_FLDNAME, VARCHAR(MAX_NAME));
		fcatSchema.addField(FCAT_TYPE, INTEGER);
		fcatSchema.addField(FCAT_TYPEARG, INTEGER);
		fcatInfo = new TableInfo(FCAT, fcatSchema);

		if (isNew) {
			formatFileHeader(TCAT, tx);
			formatFileHeader(FCAT, tx);
			createTable(TCAT, tcatSchema, tx);
			createTable(FCAT, fcatSchema, tx);
		}
	}

	/**
	 * Creates a new table having the specified name and schema.
	 * 
	 * @param tblName
	 *            the name of the new table
	 * @param sch
	 *            the table's schema
	 * @param tx
	 *            the transaction creating the table
	 */
	public void createTable(String tblName, Schema sch, Transaction tx) {
		if (tblName != TCAT_TBLNAME && tblName != FCAT_TBLNAME)
			formatFileHeader(tblName, tx);
		// Optimization: store the ti
		tiMap.put(tblName, new TableInfo(tblName, sch));

		// insert one record into tblcat
		RecordFile tcatfile = tcatInfo.open(tx, true);
		tcatfile.insert();
		tcatfile.setVal(TCAT_TBLNAME, new VarcharConstant(tblName));
		tcatfile.close();

		// insert a record into fldcat for each field
		RecordFile fcatfile = fcatInfo.open(tx, true);
		for (String fldname : sch.fields()) {
			fcatfile.insert();
			fcatfile.setVal(FCAT_TBLNAME, new VarcharConstant(tblName));
			fcatfile.setVal(FCAT_FLDNAME, new VarcharConstant(fldname));
			fcatfile.setVal(FCAT_TYPE, new IntegerConstant(sch.type(fldname)
					.getSqlType()));
			fcatfile.setVal(FCAT_TYPEARG, new IntegerConstant(sch.type(fldname)
					.getArgument()));
		}
		fcatfile.close();
	}

	/**
	 * Retrieves the metadata for the specified table out of the catalog.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the transaction
	 * @return the table's stored metadata
	 */
	public TableInfo getTableInfo(String tblName, Transaction tx) {
		// Optimization:
		TableInfo resultTi = tiMap.get(tblName);
		if (resultTi != null)
			return resultTi;

		RecordFile tcatfile = tcatInfo.open(tx, true);
		tcatfile.beforeFirst();
		boolean found = false;
		while (tcatfile.next()) {
			String t = (String) tcatfile.getVal(TCAT_TBLNAME).asJavaVal();
			if (t.equals(tblName)) {
				found = true;
				break;
			}
		}
		tcatfile.close();
		// return null if table is undefined
		if (!found)
			return null;

		RecordFile fcatfile = fcatInfo.open(tx, true);
		fcatfile.beforeFirst();
		Schema sch = new Schema();
		while (fcatfile.next())
			if (((String) fcatfile.getVal(FCAT_TBLNAME).asJavaVal())
					.equals(tblName)) {
				String fldname = (String) fcatfile.getVal(FCAT_FLDNAME)
						.asJavaVal();
				int fldtype = (Integer) fcatfile.getVal(FCAT_TYPE).asJavaVal();
				int fldarg = (Integer) fcatfile.getVal(FCAT_TYPEARG)
						.asJavaVal();
				sch.addField(fldname, Type.newInstance(fldtype, fldarg));
			}
		fcatfile.close();
		// Optimization:
		resultTi = new TableInfo(tblName, sch);
		tiMap.put(tblName, resultTi);
		return resultTi;
	}

	private void formatFileHeader(String tblName, Transaction tx) {
		String fileName = tblName + ".tbl";
		RecordFile.formatFileHeader(fileName, tx);
	}
}