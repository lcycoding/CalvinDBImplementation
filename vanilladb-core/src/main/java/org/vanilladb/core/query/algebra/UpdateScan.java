package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.record.RecordId;

/**
 * The interface implemented by all updateable scans.
 */
public interface UpdateScan extends Scan {
	/**
	 * Modifies the field value of the current record.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the new value, expressed as a Constant
	 */
	void setVal(String fldName, Constant val);

	/**
	 * Inserts a new record somewhere in the scan.
	 */
	void insert();

	/**
	 * Deletes the current record from the scan.
	 */
	void delete();

	/**
	 * Returns the RecordId of the current record.
	 * 
	 * @return the RecordId of the current record
	 */
	RecordId getRecordId();

	/**
	 * Positions the scan so that the current record has the specified record ID
	 * .
	 * 
	 * @param rid
	 *            the RecordId of the desired record
	 */
	void moveToRecordId(RecordId rid);
}
