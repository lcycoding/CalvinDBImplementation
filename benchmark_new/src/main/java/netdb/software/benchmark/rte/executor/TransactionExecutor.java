package netdb.software.benchmark.rte.executor;

import netdb.software.benchmark.TxnResultSet;
import netdb.software.benchmark.remote.SutConnection;
import netdb.software.benchmark.remote.SutResultSet;
import netdb.software.benchmark.rte.txparamgen.TxParamGenerator;

public abstract class TransactionExecutor {

	protected TxParamGenerator pg;

	public abstract TxnResultSet execute(SutConnection conn);

	protected SutResultSet callStoredProc(SutConnection spc, Object[] pars) {
		try {
			SutResultSet result = spc.callStoredProc(pg.getTxnType().ordinal(),
					pars);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

}
