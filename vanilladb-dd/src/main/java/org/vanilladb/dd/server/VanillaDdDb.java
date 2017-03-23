package org.vanilladb.dd.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.cache.naive.NaiveCacheMgr;
import org.vanilladb.dd.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.schedule.calvin.CalvinScheduler;
import org.vanilladb.dd.schedule.naive.NaiveScheduler;
import org.vanilladb.dd.storage.log.DdLogMgr;
import org.vanilladb.dd.storage.metadata.HashBasedPartitionMetaMgr;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;
import org.vanilladb.dd.util.DDProperties;

public class VanillaDdDb extends VanillaDb {
	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());

	/**
	 * The type of transactional execution engine supported by distributed
	 * deterministic VanillaDB.
	 */
	public enum ServiceType {
		NAIVE, CALVIN
	}

	private static ServiceType serviceType;

	// DD modules
	private static ConnectionMgr connMgr;
	private static PartitionMetaMgr parMetaMgr;
	private static CacheMgr cacheMgr;
	private static Scheduler scheduler;
	private static DdLogMgr ddLogMgr;

	// connection information
	private static int myNodeId;

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 * @param id
	 *            the id of the server
	 */
	public static void init(String dirName, int id) {
		myNodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("vanilladddb initializing...");

		// read service type properties
		int type = DDProperties.getLoader().getPropertyAsInteger(
				VanillaDdDb.class.getName() + ".SERVICE_TYPE",
				ServiceType.NAIVE.ordinal());
		serviceType = ServiceType.values()[type];
		if (logger.isLoggable(Level.INFO))
			logger.info("using " + serviceType + " type service");

		// initialize core modules
		VanillaDb.init(dirName, VanillaDb.BufferMgrType.DefaultBufferMgr);
		
		// initialize DD modules
		initCacheMgr();
		initPartitionMetaMgr();
		initScheduler();
		initConnectionMgr(myNodeId);
		initDdLogMgr();
	}

	// ================
	// Initializers
	// ================

	public static void initCacheMgr() {
		switch (serviceType) {
		case NAIVE:
			cacheMgr = new NaiveCacheMgr();
			break;
		case CALVIN:
			// TODO: Implement this
			cacheMgr = new CalvinCacheMgr();
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}

	public static void initScheduler() {
		switch (serviceType) {
		case NAIVE:
			scheduler = initNaiveScheduler();
			break;
		case CALVIN:
			// TODO: Implement this
			scheduler = initCalvinScheduler();
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}
	
	public static Scheduler initNaiveScheduler() {
		NaiveScheduler scheduler = new NaiveScheduler();
		taskMgr().runTask(scheduler);
		return scheduler;
	}

	// TODO: Implement this
	public static Scheduler initCalvinScheduler() {
		CalvinScheduler scheduler = new CalvinScheduler();
		taskMgr().runTask(scheduler);
		return scheduler;
	}
	

	public static void initPartitionMetaMgr() {
		Class<?> parMgrCls = DDProperties.getLoader().getPropertyAsClass(
				VanillaDdDb.class.getName() + ".PARTITION_META_MGR",
				HashBasedPartitionMetaMgr.class, PartitionMetaMgr.class);

		try {
			parMetaMgr = (PartitionMetaMgr) parMgrCls.newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}
	}

	public static void initConnectionMgr(int id) {
		connMgr = new ConnectionMgr(id);
	}

	public static void initDdLogMgr() {
		ddLogMgr = new DdLogMgr();
	}

	
	// ================
	// 	Module Getters
	// ================
	
	public static CacheMgr cacheMgr() {
		return cacheMgr;
	}

	public static Scheduler scheduler() {
		return scheduler;
	}

	public static PartitionMetaMgr partitionMetaMgr() {
		return parMetaMgr;
	}

	public static ConnectionMgr connectionMgr() {
		return connMgr;
	}

	public static DdLogMgr DdLogMgr() {
		return ddLogMgr;
	}

	
	// ===============
	// 	Other Getters
	// ===============

	public static int serverId() {
		return myNodeId;
	}

	public static ServiceType serviceType() {
		return serviceType;
	}
}
