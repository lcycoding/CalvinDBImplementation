package org.vanilladb.dd.util;

import org.vanilladb.core.util.PropertiesLoader;

public class DDProperties extends PropertiesLoader {

	private static DDProperties loader;
	
	public static DDProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new DDProperties();
		return loader;
	}
	
	protected DDProperties() {
		super();
	}
	
	@Override
	protected String getConfigFilePath() {
		return System.getProperty("org.vanilladb.dd.config.file");
	}

}
