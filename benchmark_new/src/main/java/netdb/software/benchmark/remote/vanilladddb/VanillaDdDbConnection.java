package netdb.software.benchmark.remote.vanilladddb;

import java.sql.SQLException;

import netdb.software.benchmark.remote.SutConnection;
import netdb.software.benchmark.remote.SutResultSet;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.remote.groupcomm.client.BatchGcConnection;

public class VanillaDdDbConnection implements SutConnection {
	private BatchGcConnection conn;
	private int rteId;

	public VanillaDdDbConnection(BatchGcConnection conn, int rteId) {
		this.conn = conn;
		this.rteId = rteId;
	}

	@Override
	public SutResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		SpResultSet r = conn.callStoredProc(rteId, pid, pars);
		return new VanillaDdDbResultSet(r);
	}
}
