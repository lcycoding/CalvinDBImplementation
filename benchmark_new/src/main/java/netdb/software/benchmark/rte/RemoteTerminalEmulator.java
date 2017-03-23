package netdb.software.benchmark.rte;

import netdb.software.benchmark.Benchmarker;
import netdb.software.benchmark.StatisticMgr;
import netdb.software.benchmark.TxnResultSet;
import netdb.software.benchmark.remote.SutConnection;

public abstract class RemoteTerminalEmulator extends Thread {

	private static Integer rteCount = 1;

	protected Object connArgs[];

	private volatile boolean stopBenchmark;
	private volatile boolean isWarmingUp = true;

	protected SutConnection conn;

	public void stopBenchmark() {
		stopBenchmark = true;
	}

	public RemoteTerminalEmulator(Object[] connArgs) {
		this.connArgs = connArgs;
	}

	@Override
	public void run() {
		synchronized (rteCount) {
			setName("tpcc-rte-" + rteCount);
			rteCount++;
		}

		conn = Benchmarker.getConnection(connArgs);
		StatisticMgr statMgr = Benchmarker.statMgr;

		while (!stopBenchmark) {
			TxnResultSet rs = executeTxnCycle();
			if (!isWarmingUp)
				statMgr.processTxnResult(rs);
		}
	}

	public void startRecordStatistic() {
		isWarmingUp = false;
	}

	protected abstract TxnResultSet executeTxnCycle();

	// private TxnResultSet executeTxnCycle() {
	// TransactionType type = TxnMixGenerator.nextTransactionType();
	// TpccTxnExecutor txn = executorFactory.getTxnExecutor(type, conn,
	// homeWid);
	// return txn.execute();
	// }

}