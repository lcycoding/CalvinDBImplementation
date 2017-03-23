package netdb.software.benchmark.rte;

import netdb.software.benchmark.TxnResultSet;
import netdb.software.benchmark.rte.executor.jdbc.JdbcSampleTxnExecutor;
import netdb.software.benchmark.rte.executor.jdbc.JdbcTxnExecutor;

public class JdbcRte extends RemoteTerminalEmulator {

	public JdbcRte(Object... args) {
		super(args);
	}

	@Override
	public TxnResultSet executeTxnCycle() {
	
		JdbcTxnExecutor txnExecutor = null;
		txnExecutor = new JdbcSampleTxnExecutor();
		return txnExecutor.execute();
	}

}
