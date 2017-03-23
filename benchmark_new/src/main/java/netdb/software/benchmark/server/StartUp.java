package netdb.software.benchmark.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.TestingParameters;

public class StartUp {
	private static Logger logger = Logger.getLogger(StartUp.class.getName());

	public static String dirName;

	public static void main(String[] args) throws Exception {
		loadSystemProperties();
		dirName = args[0];

		new VanillaDdDbStartUp().startup(args);
	}

	private static void loadSystemProperties() {
		boolean config = false;
		String path = System
				.getProperty("netdb.software.benchmark.tpcc.config.file");
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
				config = true;
			} catch (IOException e) {
				// do nothing
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		if (!config && logger.isLoggable(Level.WARNING))
			logger.warning("error reading the config file, using defaults");
	}
}