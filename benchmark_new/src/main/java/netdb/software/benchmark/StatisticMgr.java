package netdb.software.benchmark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class
			.getName());

	private static final String OUTPUT_DIR;

	private List<TxnResultSet> resultSets = new ArrayList<TxnResultSet>();

	static {
		String prop = System.getProperty(StatisticMgr.class.getName()
				+ ".OUTPUT_DIR");
		OUTPUT_DIR = (prop == null ? System.getProperty("user.home") : prop
				.trim());
	}

	/**
	 * Record the input transaction result for further statistic calculation
	 * 
	 * @param trs
	 */
	public synchronized void processTxnResult(TxnResultSet trs) {
		resultSets.add(trs);
	}

	public synchronized void processBatchTxnsResult(TxnResultSet... trss) {
		for (TxnResultSet trs : trss)
			resultSets.add(trs);
	}

	/**
	 * Calculate and print out the result into the result files from the
	 * recorded transaction results
	 */
	public synchronized void outputReport() {
		HashMap<TransactionType, TxnStatistic> txnStatistics = new HashMap<TransactionType, TxnStatistic>();
		txnStatistics.put(TransactionType.MICROBENCHMARK_TXN, new TxnStatistic(
				TransactionType.MICROBENCHMARK_TXN));

		try {
			File dir = new File(OUTPUT_DIR);
			File outputFile = new File(dir.getAbsoluteFile(), System.nanoTime() + ".txt");
			FileWriter wrFile = new FileWriter(outputFile);
			BufferedWriter bwrFile = new BufferedWriter(wrFile);

			// write total transaction count
			bwrFile.write("# of txns during benchmark period: "
					+ resultSets.size());
			bwrFile.newLine();

			// read all txn resultset
			for (TxnResultSet resultSet : resultSets) {
				bwrFile.write(resultSet.getTxnType()
						+ ": "
						+ TimeUnit.NANOSECONDS.toMillis(resultSet
								.getTxnResponseTime()) + " ms");
				bwrFile.newLine();
				TxnStatistic txnStatistic = txnStatistics.get(resultSet
						.getTxnType());
				if (txnStatistic != null)
					txnStatistic.addTxnResponseTime(resultSet
							.getTxnResponseTime());
			}
			bwrFile.newLine();

			// output result
			for (Entry<TransactionType, TxnStatistic> entry : txnStatistics
					.entrySet()) {
				TxnStatistic value = entry.getValue();
				if (value.txnCount > 0) {
					long avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(value
							.getTotalResponseTime() / value.txnCount);
					bwrFile.write(value.getmType() + " " + value.getTxnCount()
							+ " avg latency: " + avgResTimeMs + " ms");
				} else {
					bwrFile.write(value.getmType() + " " + value.getTxnCount()
							+ " avg latency: 0 ms");
				}
				bwrFile.newLine();
			}
			bwrFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finnish creating tpcc benchmark report at '"
					+ new File(OUTPUT_DIR).getAbsolutePath() + "'");
	}

	private static class TxnStatistic {
		private TransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(TransactionType txnType) {
			this.mType = txnType;
		}

		public TransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}
}
