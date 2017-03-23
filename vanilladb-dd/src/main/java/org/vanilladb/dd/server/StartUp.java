package org.vanilladb.dd.server;

import java.util.logging.Level;
import java.util.logging.Logger;

public class StartUp {
	private static Logger logger = Logger.getLogger(StartUp.class.getName());

	public static void main(String args[]) throws Exception {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		// configure and initialize the database
		VanillaDdDb.init(args[0], Integer.parseInt(args[1]));

		if (logger.isLoggable(Level.INFO))
			logger.info("dd database server ready");
	}
}
