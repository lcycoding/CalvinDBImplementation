package netdb.software.benchmark;

public class TestingParameters {
	public static final int SUT_VANILLA_DB = 1, SUT_VANILLA_DDDB = 2;

	public static final long BENCHMARK_INTERVAL;
	public static final long WARM_UP_INTERVAL;
	public static final int SUT;
	public static final int NUM_RTES;

	public static final boolean IS_MICROBENCHMARK;

	static {
		String prop = System.getProperty(TestingParameters.class.getName()
				+ ".BENCHMARK_INTERVAL");
		BENCHMARK_INTERVAL = (prop == null ? 100000 : Integer.parseInt(prop
				.trim()));
		prop = System.getProperty(TestingParameters.class.getName()
				+ ".WARM_UP_INTERVAL");
		WARM_UP_INTERVAL = (prop == null ? 30000 : Integer
				.parseInt(prop.trim()));
		prop = System.getProperty(TestingParameters.class.getName() + ".SUT");
		SUT = (prop == null ? 1 : Integer.parseInt(prop.trim()));
		prop = System.getProperty(TestingParameters.class.getName()
				+ ".NUM_RTES");
		NUM_RTES = (prop == null ? 1 : Integer.parseInt(prop.trim()));

		prop = System.getProperty(TestingParameters.class.getName()
				+ ".IS_MICROBENCHMARK");
		IS_MICROBENCHMARK = (prop == null) ? false : Boolean.parseBoolean(prop
				.trim());
	}
}