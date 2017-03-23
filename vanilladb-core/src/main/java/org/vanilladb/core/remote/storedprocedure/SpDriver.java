package org.vanilladb.core.remote.storedprocedure;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.Properties;

public class SpDriver {

	public SpConnection connect(String url, Properties prop)
			throws SQLException {
		try {
			// assumes no port specified
			String host = url.replace("sp:vanilladb://", "");
			Registry reg = LocateRegistry.getRegistry(host);
			RemoteDriver rdvr = (RemoteDriver) reg.lookup("vanilladb-sp");
			RemoteConnection rconn = rdvr.connect();
			return new SpConnection(rconn);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
