package org.vanilladb.core.query.algebra.index;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;

/**
 * The scan class corresponding to the indexjoin relational algebra operator.
 * The code is very similar to that of ProductScan, which makes sense because an
 * index join is essentially the product of each LHS record with the matching
 * RHS index records.
 */
public class IndexJoinScan implements Scan {
	private Scan s;
	private TableScan ts; // the data table
	private Index idx;
	private String joinField;
	private boolean isLhsEmpty;

	/**
	 * Creates an index join scan for the specified LHS scan and RHS index.
	 * 
	 * @param s
	 *            the LHS scan
	 * @param idx
	 *            the RHS index
	 * @param joinField
	 *            the LHS field used for joining
	 * @param ts
	 *            the table scan of data table
	 */
	public IndexJoinScan(Scan s, Index idx, String joinField, TableScan ts) {
		this.s = s;
		this.idx = idx;
		this.joinField = joinField;
		this.ts = ts;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first record. That is, the LHS scan will be
	 * positioned at its first record, and the index will be positioned before
	 * the first record for the join value.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		s.beforeFirst();
		isLhsEmpty = !s.next();// in the case that s may be empty
		if (!isLhsEmpty)
			resetIndex();
	}

	/**
	 * Moves the scan to the next record. The method moves to the next index
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first index record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)
			return false;
		if (idx.next()) {
			ts.moveToRecordId(idx.getDataRecordId());
			return true;
		} else if (!(isLhsEmpty = !s.next())) {
			resetIndex();
			return next();
		} else
			return false;
	}

	/**
	 * Closes the scan by closing its LHS scan and its RHS index.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s.close();
		idx.close();
		ts.close();
	}

	/**
	 * Returns the Constant value of the specified field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (ts.hasField(fldName))
			return ts.getVal(fldName);
		else
			return s.getVal(fldName);
	}

	/**
	 * Returns true if the field is in the schema.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return ts.hasField(fldName) || s.hasField(fldName);
	}

	private void resetIndex() {
		Constant searchkey = s.getVal(joinField);
		idx.beforeFirst(ConstantRange.newInstance(searchkey));
	}

}
