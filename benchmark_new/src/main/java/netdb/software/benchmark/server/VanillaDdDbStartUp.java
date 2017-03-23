package netdb.software.benchmark.server;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.App;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.server.VanillaDdDb;

public class VanillaDdDbStartUp implements SutStartUp {
	private static Logger logger = Logger.getLogger(VanillaDdDbStartUp.class
			.getName());

	public void startup(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		VanillaDdDb.init(args[0], Integer.parseInt(args[1]));
		

		if (logger.isLoggable(Level.INFO))
			logger.info("tpcc benchmark vanilladb-dd server ready");
	}

}
