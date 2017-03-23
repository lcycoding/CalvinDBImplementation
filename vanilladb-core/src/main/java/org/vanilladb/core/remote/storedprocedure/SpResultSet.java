package org.vanilladb.core.remote.storedprocedure;

import java.io.Serializable;

import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

public class SpResultSet implements Serializable {

	private static final long serialVersionUID = -8409489171990111489L;
	private Record[] records;
	private Schema schema;

	public SpResultSet(Schema schema, Record... records) {
		this.records = records;
		this.schema = schema;
	}

	public Record[] getRecords() {
		return records;
	}

	public Schema getSchema() {
		return schema;
	}
}
