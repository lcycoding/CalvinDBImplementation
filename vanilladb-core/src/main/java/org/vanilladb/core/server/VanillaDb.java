package org.vanilladb.core.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.server.task.TaskMgr;
import org.vanilladb.core.sql.storedprocedure.SampleStoredProcedureFactory;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.buffer.BufferMgrImpl;
import org.vanilladb.core.storage.buffer.DummyBufferMgr;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionMgr;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.util.Profiler;

/**
 * The class that provides system-wide static global values. These values must
 * be initialized by the method {@link #init(String) init} before use. The
 * methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr}, and
 * {@link #initCatalogMgr(boolean, Transaction) initCatalogMgr} provide limited
 * initialization, and are useful for debugging purposes.
 */
public class VanillaDb {

	public static enum BufferMgrType {
		DefaultBufferMgr, DummyBufferMgr
	};

	// Logger
	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());

	// Classes
	private static Class<?> queryPlannerCls, updatePlannerCls;

	// Managers
	private static FileMgr fileMgr;
	private static BufferMgr buffMgr;
	private static LogMgr logMgr;
	private static CatalogMgr catalogMgr;
	private static StatMgr statMgr;
	private static TaskMgr taskMgr;
	private static TransactionMgr txMgr;

	// Utility classes
	private static StoredProcedureFactory spFactory;
	private static Profiler profiler;

	/**
	 * Initialization Flag
	 */
	private static boolean inited;

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void init(String dirName) {
		init(dirName, BufferMgrType.DefaultBufferMgr);
	}

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 * @param bufferType
	 *            the type of the buffer manager for storage engine
	 */
	public static void init(String dirName, BufferMgrType bufferType) {

		if (inited) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("discarding duplicated init request");
			return;
		}

		/*
		 * Note: We read properties file here before, but we moved it into a
		 * utility class, PropertiesFetcher, for safety reason.
		 */

		// read classes
		queryPlannerCls = CoreProperties.getLoader().getPropertyAsClass(
				VanillaDb.class.getName() + ".QUERYPLANNER",
				HeuristicQueryPlanner.class, QueryPlanner.class);
		updatePlannerCls = CoreProperties.getLoader().getPropertyAsClass(
				VanillaDb.class.getName() + ".UPDATEPLANNER",
				IndexUpdatePlanner.class, UpdatePlanner.class);

		// initialize storage engine
		initFileLogAndBufferMgr(dirName, bufferType);
		initTaskMgr();
		initTxMgr();

		// the first transaction for initializing the system
		Transaction initTx = txMgr.newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		/*
		 * initialize the catalog manager to ensure the recovery process can get
		 * the index info (required for index logical recovery)
		 */
		boolean isDbNew = fileMgr.isNew();
		initCatalogMgr(isDbNew, initTx);
		if (isDbNew) {
			if (logger.isLoggable(Level.INFO))
				logger.info("creating new database");
		} else {
			if (logger.isLoggable(Level.INFO))
				logger.info("recovering existing database");
			// add a checkpoint record to limit rollback
			RecoveryMgr.recover(initTx);
			logMgr.removeAndCreateNewLog();
		}

		// initialize the statistics manager to build the histogram
		initStatMgr(initTx);

		// initialize SP Factory
		initSPFactory();

		// commit the initializing transaction
		initTx.commit();

		// initializing checkpointing task
		boolean doCheckpointing = CoreProperties.getLoader().getPropertyAsBoolean(
				VanillaDb.class.getName() + ".DO_CHECKPOINT", true);
		if (doCheckpointing)
			initCheckpointingTask();

		// finish initialization
		inited = true;
	}

	/**
	 * Is VanillaDB initialized ?
	 * 
	 * @return true if it is initialized, otherwise false.
	 */
	public static boolean isInited() {
		return inited;
	}

	/*
	 * The following initialization methods are useful for testing the
	 * lower-level components of the system without having to initialize
	 * everything.
	 */

	/**
	 * Initializes only the file manager.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileMgr(String dirName) {
		fileMgr = new FileMgr(dirName);
	}

	/**
	 * Initializes the file and log managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileAndLogMgr(String dirName) {
		initFileMgr(dirName);
		logMgr = new LogMgr();
	}

	/**
	 * Initializes the file, log, and buffer managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileLogAndBufferMgr(String dirName,
			BufferMgrType bufferMgrType) {
		initFileAndLogMgr(dirName);

		if (bufferMgrType == BufferMgrType.DefaultBufferMgr)
			buffMgr = new BufferMgrImpl();
		else
			buffMgr = new DummyBufferMgr();

		Transaction.addStartListener(buffMgr);
	}

	/**
	 * Initializes the task manager.
	 */
	public static void initTaskMgr() {
		taskMgr = new TaskMgr();
	}

	/**
	 * Initializes the transaction manager.
	 */
	public static void initTxMgr() {
		txMgr = new TransactionMgr();
	}

	/**
	 * Initializes the catalog manager. Note that the catalog manager should be
	 * initialized <em>before</em> system recovery.
	 * 
	 * @param isNew
	 *            an indication of whether a new database needs to be created.
	 * @param tx
	 *            the transaction performing the initialization
	 */
	public static void initCatalogMgr(boolean isNew, Transaction tx) {
		catalogMgr = new CatalogMgr(isNew, tx);
	}

	/**
	 * Initializes the statistics manager. Note that this manager should be
	 * initialized <em>after</em> system recovery.
	 * 
	 * @param tx
	 *            the transaction performing the initialization
	 */
	public static void initStatMgr(Transaction tx) {
		statMgr = new StatMgr(tx);
	}

	/**
	 * Initializes a store procedure factory for creating stored procedures.
	 */
	public static void initSPFactory() {
		Class<?> spFactoryCls = CoreProperties.getLoader().getPropertyAsClass(
				VanillaDb.class.getName() + ".SP_FACTORY",
				SampleStoredProcedureFactory.class,
				StoredProcedureFactory.class);
		if (spFactoryCls != null) {
			try {
				spFactory = (StoredProcedureFactory) spFactoryCls.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Initialize a background checkpointing task.
	 */
	public static void initCheckpointingTask() {
		taskMgr.runTask(new CheckpointTask());
	}

	public static FileMgr fileMgr() {
		return fileMgr;
	}

	public static BufferMgr bufferMgr() {
		return buffMgr;
	}

	public static LogMgr logMgr() {
		return logMgr;
	}

	public static CatalogMgr catalogMgr() {
		return catalogMgr;
	}

	public static StatMgr statMgr() {
		return statMgr;
	}

	public static TaskMgr taskMgr() {
		return taskMgr;
	}

	public static TransactionMgr txMgr() {
		return txMgr;
	}

	public static StoredProcedureFactory spFactory() {
		return spFactory;
	}

	/**
	 * Creates a planner for SQL commands. To change how the planner works,
	 * modify this method.
	 * 
	 * @return the system's planner for SQL commands
	 */
	public static Planner newPlanner() {
		QueryPlanner qplanner;
		UpdatePlanner uplanner;

		try {
			qplanner = (QueryPlanner) queryPlannerCls.newInstance();
			uplanner = (UpdatePlanner) updatePlannerCls.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}

		return new Planner(qplanner, uplanner);
	}

	/**
	 * Initialize profiler and start collecting profiling data.
	 */
	public static void initAndStartProfiler() {
		profiler = new Profiler();
		profiler.startCollecting();
	}

	/**
	 * Stop profiler and generate report file.
	 */
	public static void stopProfilerAndReport() {
		profiler.stopCollecting();

		// Write a report file
		try {
			// Get path from property file
			String path = CoreProperties.getLoader().getPropertyAsString(
					VanillaDb.class.getName() + ".PROFILE_OUTPUT_DIR",
					System.getProperty("user.home"));
			File out = new File(path, System.currentTimeMillis()
					+ "_profile.txt");
			FileWriter wrFile = new FileWriter(out);
			BufferedWriter bwrFile = new BufferedWriter(wrFile);

			// Write Profiling Report
			bwrFile.write(profiler.getTopPackages(30));
			bwrFile.newLine();
			bwrFile.write(profiler.getTopMethods(30));
			bwrFile.newLine();
			bwrFile.write(profiler.getTopLines(30));

			/*
			 * I should write a more careful code here. I didn't do it, because
			 * of the same reason above.
			 */
			bwrFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
