package netdb.software.benchmark;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.remote.SutConnection;
import netdb.software.benchmark.remote.SutDriver;
import netdb.software.benchmark.remote.vanilladddb.VanillaDdDbDriver;
import netdb.software.benchmark.rte.RemoteTerminalEmulator;
import netdb.software.benchmark.rte.tpcc.SimpleRte;
import netdb.software.benchmark.util.RandomValueGenerator;

public class Benchmarker {
	private static Logger logger = Logger.getLogger(Benchmarker.class
			.getName());

	public static RandomValueGenerator generator;
	public static StatisticMgr statMgr;
	public static SutDriver driver;

	public Benchmarker() {
		statMgr = new StatisticMgr();
		generator = new RandomValueGenerator();
		initDriver();
	}

	/**
	 * Load testbed
	 */
	public void load() {
		if (logger.isLoggable(Level.INFO))
			logger.info("loading the testbed of tpcc benchmark...");
		try {
			SutConnection spc = Benchmarker.getConnection(0);
			spc.callStoredProc(TransactionType.SCHEMA_BUILDER.ordinal());
			spc.callStoredProc(TransactionType.TESTBED_LOADER.ordinal());
			// spc.callStoredProc(MYSQL_TESTBED_LOADER.pid());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Start running benchmark
	 */
	public void run() {
		if (logger.isLoggable(Level.INFO))
			logger.info("running tpcc benchmark...");

		// Initialize RTEs
		RemoteTerminalEmulator[] emulators = new RemoteTerminalEmulator[TestingParameters.NUM_RTES];
		for (int i = 0; i < emulators.length; i++) {
			emulators[i] = getRte(i);
		}

		try {
			Thread.sleep(1500);
			
			// start each RTEs
			for (int i = 0; i < emulators.length; i++) {
				emulators[i].start();
				System.out.println("RTE start: " + i);
			}

			// sleep for warm up time
			Thread.sleep(TestingParameters.WARM_UP_INTERVAL);

			// start recording each RTEs result
			for (int i = 0; i < emulators.length; i++)
				emulators[i].startRecordStatistic();
			
			// benchmark finished
			Thread.sleep(TestingParameters.BENCHMARK_INTERVAL);
			for (int i = 0; i < emulators.length; i++)
				emulators[i].stopBenchmark();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Tpcc benchmark finished...");
	}

	/**
	 * Output report
	 */
	public void report() {
		statMgr.outputReport();
	}

	private void initDriver() {
		driver = new VanillaDdDbDriver();
	}

	public static SutConnection getConnection(Object... args) {
		
		if(driver == null)
			return null;
		
		try {
			return driver.connectToSut(args);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get a remote terminal emulator
	 * @param args
	 * @return
	 */
	private RemoteTerminalEmulator getRte(Object... args) {
		return new SimpleRte(args);
	}
}