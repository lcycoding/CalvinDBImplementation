package org.vanilladb.core.storage.record;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.record.RecordPage.EMPTY;

import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;

/**
 * An object that can format a page to look like a block of empty records.
 */
public class RecordFormatter implements PageFormatter {
	private TableInfo ti;
	private Map<String, Integer> myOffsetMap;
	// Optimization: store the size of pointer to other log record
	private int flagSize = Page.maxSize(INTEGER);

	/**
	 * Creates a formatter for a new page of a table.
	 * 
	 * @param ti
	 *            the table's metadata
	 */
	public RecordFormatter(TableInfo ti) {
		this.ti = ti;
		myOffsetMap = RecordPage.offsetMap(ti.schema());
	}

	/**
	 * Formats the page by allocating as many record slots as possible, given
	 * the record size. Each record slot is assigned a flag of EMPTY. Each
	 * numeric field is given a value of 0, and each string field is given a
	 * value of "".
	 * 
	 * @see org.vanilladb.core.storage.buffer.PageFormatter#format(org.vanilladb.core.storage.file.Page)
	 */
	@Override
	public void format(Page page) {
		int slotSize = RecordPage.slotSize(ti.schema());
		Constant emptyFlag = new IntegerConstant(EMPTY);
		for (int pos = 0; pos + slotSize <= BLOCK_SIZE; pos += slotSize) {
			page.setVal(pos, emptyFlag);
			makeDefaultRecord(page, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		int offset;
		for (String fldname : ti.schema().fields()) {
			offset = myOffsetMap.get(fldname);
			page.setVal(pos + flagSize + offset,
					Constant.defaultInstance(ti.schema().type(fldname)));
		}
	}
}
