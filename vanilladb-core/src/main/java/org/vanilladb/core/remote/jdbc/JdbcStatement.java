package org.vanilladb.core.remote.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteStatement. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class JdbcStatement extends StatementAdapter {
	private RemoteStatement rstmt;

	public JdbcStatement(RemoteStatement s) {
		rstmt = s;
	}

	public ResultSet executeQuery(String qry) throws SQLException {
		try {
			RemoteResultSet rrs = rstmt.executeQuery(qry);
			return new JdbcResultSet(rrs);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public int executeUpdate(String cmd) throws SQLException {
		try {
			return rstmt.executeUpdate(cmd);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

}
