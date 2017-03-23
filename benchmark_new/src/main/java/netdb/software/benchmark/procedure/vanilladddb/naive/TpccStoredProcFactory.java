package netdb.software.benchmark.procedure.vanilladddb.naive;

import netdb.software.benchmark.TransactionType;

import org.vanilladb.dd.schedule.naive.NaiveStoredProcedure;
import org.vanilladb.dd.schedule.naive.NaiveStoredProcedureFactory;

public class TpccStoredProcFactory implements NaiveStoredProcedureFactory {

	@Override
	public NaiveStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		NaiveStoredProcedure<?> sp;
		switch (TransactionType.values()[pid]) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case MICROBENCHMARK_TXN:
			sp = new MicroBenchmarkProc(txNum);
			break;
		default:
			sp = null;
		}
		return sp;
	}
}
