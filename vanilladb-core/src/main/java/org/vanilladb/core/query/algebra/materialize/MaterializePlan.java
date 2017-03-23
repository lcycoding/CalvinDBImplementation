package org.vanilladb.core.query.algebra.materialize;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class for the <em>materialize</em> operator.
 */
public class MaterializePlan implements Plan {
	private Plan p;
	private Transaction tx;

	/**
	 * Creates a materialize plan for the specified query.
	 * 
	 * @param p
	 *            the plan of the underlying query
	 * @param tx
	 *            the calling transaction
	 */
	public MaterializePlan(Plan p, Transaction tx) {
		this.p = p;
		this.tx = tx;
	}

	/**
	 * This method loops through the underlying query, copying its output
	 * records into a temporary table. It then returns a table scan for that
	 * table.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Schema sch = p.schema();
		TempTable temp = new TempTable(sch, tx);
		Scan src = p.open();
		UpdateScan dest = temp.open();
		src.beforeFirst();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.beforeFirst();
		return dest;
	}

	/**
	 * Returns the estimated number of blocks in the materialized table. It does
	 * <em>not</em> include the one-time cost of materializing the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		double rpb = (double) (BLOCK_SIZE / RecordPage.slotSize(p.schema()));
		return (int) Math.ceil(p.recordsOutput() / rpb);
	}

	/**
	 * Returns the schema of the materialized table, which is the same as in the
	 * underlying plan.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return p.schema();
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return p.recordsOutput();
	}

	@Override
	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("MaterializePlan: (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
