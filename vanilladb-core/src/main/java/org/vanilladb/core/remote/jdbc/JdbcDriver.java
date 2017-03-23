package org.vanilladb.core.remote.jdbc;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The VanillaDb database driver for JDBC.
 */
public class JdbcDriver extends DriverAdapter {

	/**
	 * Connects to the VanillaDb server on the specified host. The method
	 * retrieves the RemoteDriver stub from the RMI registry on the specified
	 * host. It then calls the connect method on that stub, which in turn
	 * creates a new connection and returns the RemoteConnection stub for it.
	 * This stub is wrapped in a {@link JdbcConnection} object and is returned.
	 * <P>
	 * The current implementation of this method ignores the properties
	 * argument.
	 * 
	 * @see java.sql.Driver#connect(java.lang.String, Properties)
	 */
	public Connection connect(String url, Properties prop) throws SQLException {
		try {
			// assumes no port specified
			String host = url.replace("jdbc:vanilladb://", "");
			Registry reg = LocateRegistry.getRegistry(host);
			RemoteDriver rdvr = (RemoteDriver) reg.lookup(JdbcStartUp.RMI_REG_NAME);
			RemoteConnection rconn = rdvr.connect();
			return new JdbcConnection(rconn);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
