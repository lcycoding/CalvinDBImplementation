package org.vanilladb.core.remote.storedprocedure;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class SpStartUp {
	/**
	 * Starts up the stored procedure call driver in server side by binding the
	 * remote driver object to local registry.
	 * 
	 * @throws Exception
	 */
	public static void startUp() throws Exception {
		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("vanilladb-sp", d);
	}
}
