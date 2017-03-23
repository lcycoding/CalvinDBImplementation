package netdb.software.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
	// the node id used in vanilladb-dd
	public static int myNodeId;

	private static Logger logger = Logger.getLogger(App.class.getName());
	static boolean LOAD_TESTBED, PROFILE, RUN_BENCHMARK;

	public static void main(String[] args) {
		loadSystemProperties();
		myNodeId = Integer.parseInt(args[0]);
		Benchmarker b = new Benchmarker();

		if (LOAD_TESTBED && myNodeId == 0)
			b.load();

		if (RUN_BENCHMARK) {
			b.run();

			if (logger.isLoggable(Level.INFO))
				logger.info("benchmark period end...");
		}
		b.report();
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

		String prop = System.getProperty(App.class.getName() + ".LOAD_TESTBED");
		LOAD_TESTBED = (prop == null) ? false : Boolean.parseBoolean(prop
				.trim());

		prop = System.getProperty(App.class.getName() + ".RUN_BENCHMARK");
		RUN_BENCHMARK = (prop == null) ? true : Boolean.parseBoolean(prop
				.trim());
	}
}
