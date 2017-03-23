package org.vanilladb.core.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.remote.jdbc.JdbcStartUp;

public class StartUp {
	private static Logger logger = Logger.getLogger(StartUp.class.getName());

	public static void main(String args[]) throws Exception {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		// configure and initialize the database
		VanillaDb.init(args[0]);

		// start up the listening port
		JdbcStartUp.startUp(1099);

		if (logger.isLoggable(Level.INFO))
			logger.info("database server ready");
	}
}
