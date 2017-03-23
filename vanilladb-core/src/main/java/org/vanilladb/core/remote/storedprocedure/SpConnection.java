package org.vanilladb.core.remote.storedprocedure;

import java.sql.SQLException;

public class SpConnection {
	private RemoteConnection conn;

	public SpConnection(RemoteConnection conn) {
		this.conn = conn;
	}

	public SpResultSet callStoredProc(int pid, Object... pars)
			throws SQLException {
		try {
			return conn.callStoredProc(pid, pars);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
