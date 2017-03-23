package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

/**
 * The scan class corresponding to the <em>explain</em> relational algebra
 * operator.
 */
public class ExplainScan implements Scan {

	private String result;
	private int numRecs;
	private Schema schema;
	private boolean isBeforeFirst;

	/**
	 * Creates a explain scan having the specified underlying query.
	 * 
	 * @param s
	 *            the scan of the underlying query
	 * @param schema
	 *            the schema of the explain result
	 * @param explain
	 *            the string that explains the underlying query's planning tree
	 */
	public ExplainScan(Scan s, Schema schema, String explain) {
		this.result = "\n" + explain;
		this.schema = schema;
		s.beforeFirst();
		while (s.next())
			numRecs++;
		s.close();
		this.result = result + "\nActual #recs: " + numRecs;
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan")) {
			return new VarcharConstant(result);
		} else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		isBeforeFirst = true;
	}

	@Override
	public boolean next() {
		if (isBeforeFirst) {
			isBeforeFirst = false;
			return true;
		} else
			return false;
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public boolean hasField(String fldname) {
		return schema.hasField(fldname);
	}
}
