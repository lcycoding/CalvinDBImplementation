package netdb.software.benchmark.remote.vanilladb;

import java.sql.SQLException;

import netdb.software.benchmark.remote.SutConnection;
import netdb.software.benchmark.remote.SutDriver;

import org.vanilladb.core.remote.storedprocedure.SpDriver;

public class VanillaDbDriver implements SutDriver {

	private static final String SERVER_IP;

	static {
		String prop = System.getProperty(VanillaDbDriver.class.getName()
				+ ".SERVER_IP");
		SERVER_IP = prop == null ? "" : prop.trim();
	}

	public SutConnection connectToSut(Object... args) throws SQLException {
		try {
			SpDriver driver = new SpDriver();
			return new VanillaDbConnection(driver.connect(SERVER_IP, null));
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
