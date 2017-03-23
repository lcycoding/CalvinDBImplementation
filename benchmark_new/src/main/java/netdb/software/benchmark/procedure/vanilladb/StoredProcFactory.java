package netdb.software.benchmark.procedure.vanilladb;

import netdb.software.benchmark.TransactionType;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory;

public class StoredProcFactory implements StoredProcedureFactory {

	@Override
	public StoredProcedure getStroredProcedure(int pid) {
		StoredProcedure sp;
		switch (TransactionType.values()[pid]) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc();
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc();
			break;
		case MICROBENCHMARK_TXN:
			sp = new MicroBenchmarkProc();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}