package netdb.software.benchmark.remote.vanilladddb;

import netdb.software.benchmark.remote.SutResultSet;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

public class VanillaDdDbResultSet implements SutResultSet {
	private Record[] recs;
	private Schema sch;

	public VanillaDdDbResultSet(SpResultSet result) {
		if(result != null){
			recs = result.getRecords();
			sch = result.getSchema();
		}
	}

	public boolean isCommitted() {
		if (!sch.hasField("status"))
			throw new RuntimeException("result set not completed");
		String status = (String) recs[0].getVal("status").asJavaVal();
		return status.equals("committed");
	}

	public String outputMsg() {
		return recs[0].toString();
	}
}
