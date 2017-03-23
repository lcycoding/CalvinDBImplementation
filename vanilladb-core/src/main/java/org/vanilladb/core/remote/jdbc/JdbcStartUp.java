package org.vanilladb.core.remote.jdbc;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class JdbcStartUp {
	
	public static String RMI_REG_NAME = "vanilladb-jdbc";

	/**
	 * Starts up the JDBC driver in server side by binding the remote driver
	 * object to local registry.
	 * 
	 * @throws Exception
	 */
	public static void startUp(int port) throws Exception {
		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(port);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind(RMI_REG_NAME, d);
	}
}
