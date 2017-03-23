package netdb.software.benchmark.rte.executor.jdbc;

import netdb.software.benchmark.TransactionType;
import netdb.software.benchmark.TxnResultSet;

public abstract class JdbcTxnExecutor {

	/**
	 * Prepare the parameters for further execution.
	 */
	protected abstract void prepareParams();

	protected abstract void executeSql();

	protected abstract TransactionType getTrasactionType();

	public TxnResultSet execute() {
		TxnResultSet rs = new TxnResultSet();
		rs.setTxnType(getTrasactionType());

		// generate parameters
		prepareParams();

		// send txn request and start measure txn response time
		long txnRT = System.nanoTime();
		executeSql();

		// measure txn response time
		txnRT = System.nanoTime() - txnRT;

		// display output
		System.out.println(getTrasactionType() + " done.");

		rs.setTxnResponseTimeNs(txnRT);

		return rs;
	}
}
