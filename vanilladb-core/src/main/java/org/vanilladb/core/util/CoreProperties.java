package org.vanilladb.core.util;

public class CoreProperties extends PropertiesLoader {

	private static CoreProperties loader;
	
	public static CoreProperties getLoader() {
		// Singleton
		if (loader == null)
			loader = new CoreProperties();
		return loader;
	}
	
	protected CoreProperties() {
		super();
	}
	
	@Override
	protected String getConfigFilePath() {
		return System.getProperty("org.vanilladb.core.config.file");
	}

}
