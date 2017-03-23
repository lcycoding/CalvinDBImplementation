package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.Map;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;

/**
 * Formats a B-tree page.
 * 
 * @see BTreePage
 */
public class BTPageFormatter implements PageFormatter {
	private TableInfo ti;
	private Map<String, Integer> myOffsetMap;
	private long[] flags;

	/**
	 * Creates a formatter.
	 * 
	 * @param ti
	 *            the index's metadata
	 * @param flags
	 *            the page's flag values
	 */
	public BTPageFormatter(TableInfo ti, long[] flags) {
		this.ti = ti;
		this.flags = flags;
		myOffsetMap = BTreePage.offsetMap(ti.schema());
	}

	/**
	 * Formats the page by initializing as many index-record slots as possible
	 * to have default values.
	 * 
	 * @see PageFormatter#format(Page)
	 */
	@Override
	public void format(Page page) {
		int pos = 0;
		// initial the number of records as 0
		page.setVal(pos, Constant.defaultInstance(INTEGER));
		int flagSize = Page.maxSize(BIGINT);
		pos += Page.maxSize(INTEGER);
		// set flags
		for (int i = 0; i < flags.length; i++) {
			page.setVal(pos, new BigIntConstant(flags[i]));
			pos += flagSize;
		}
		int slotSize = BTreePage.slotSize(ti.schema());
		for (int p = pos; p + slotSize <= BLOCK_SIZE; p += slotSize)
			makeDefaultRecord(page, p);
	}

	private void makeDefaultRecord(Page page, int pos) {
		int offset;
		for (String fldname : ti.schema().fields()) {
			offset = myOffsetMap.get(fldname);
			page.setVal(pos + offset,
					Constant.defaultInstance(ti.schema().type(fldname)));
		}
	}
}
