package netdb.software.benchmark.rte.tpcc;

import netdb.software.benchmark.TxnResultSet;
import netdb.software.benchmark.rte.RemoteTerminalEmulator;
import netdb.software.benchmark.rte.executor.MicrobenchmarkTxnExecutor;
import netdb.software.benchmark.rte.executor.TransactionExecutor;
import netdb.software.benchmark.rte.txparamgen.MicrobenchmarkParamGen;

public class SimpleRte extends RemoteTerminalEmulator {

	private MicrobenchmarkParamGen paramGem;

	public SimpleRte(Object[] connArgs) {
		super(connArgs);
		paramGem = new MicrobenchmarkParamGen();
	}

	@Override
	protected TxnResultSet executeTxnCycle() {
		TransactionExecutor tx = new MicrobenchmarkTxnExecutor(paramGem);
		return tx.execute(conn);
	}

}
