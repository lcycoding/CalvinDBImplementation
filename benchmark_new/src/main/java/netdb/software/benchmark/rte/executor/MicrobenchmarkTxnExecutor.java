package netdb.software.benchmark.rte.executor;

import netdb.software.benchmark.TxnResultSet;
import netdb.software.benchmark.remote.SutConnection;
import netdb.software.benchmark.remote.SutResultSet;
import netdb.software.benchmark.rte.txparamgen.TxParamGenerator;

public class MicrobenchmarkTxnExecutor extends TransactionExecutor {

	public MicrobenchmarkTxnExecutor(TxParamGenerator pg) {
		this.pg = pg;
	}

	public TxnResultSet execute(SutConnection conn) {
		try {
			TxnResultSet rs = new TxnResultSet();
			rs.setTxnType(pg.getTxnType());

			// generate parameters
			Object[] params = pg.generateParameter();

			// send txn request and start measure txn response time
			long txnRT = System.nanoTime();
			SutResultSet result = callStoredProc(conn, params);

			// measure txn response time
			txnRT = System.nanoTime() - txnRT;

			// display output
			System.out.println(pg.getTxnType() + " " + result.outputMsg());

			rs.setTxnIsCommited(result.isCommitted());
			rs.setOutMsg(result.outputMsg());
			rs.setTxnResponseTimeNs(txnRT);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
}
