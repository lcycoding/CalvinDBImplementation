package org.vanilladb.core.storage.tx.recovery;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * The task performs non-quiescent checkpointing.
 */
public class CheckpointTask extends Task {
	private static Logger logger = Logger.getLogger(CheckpointTask.class
			.getName());

	private static final int TX_COUNT_TO_CHECKPOINT;
	private static final int METHOD_PERIODIC = 0, METHOD_MONITOR = 1;
	private static final int MY_METHOD;
	private static final long PERIOD;
	private long lastTxNum;

	static {
		TX_COUNT_TO_CHECKPOINT = CoreProperties.getLoader()
				.getPropertyAsInteger(CheckpointTask.class.getName()
						+ ".TX_COUNT_TO_CHECKPOINT", 1000);
		MY_METHOD = CoreProperties.getLoader().getPropertyAsInteger(
				CheckpointTask.class.getName() + ".MY_METHOD", METHOD_PERIODIC);
		PERIOD = CoreProperties.getLoader().getPropertyAsLong(
				CheckpointTask.class.getName() + ".PERIOD", 300000);
	}

	public CheckpointTask() {

	}

	/**
	 * Create a non-quiescent checkpoint.
	 */
	public void createCheckpoint() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start creating checkpoint");
		if (MY_METHOD == METHOD_MONITOR) {
			if (VanillaDb.txMgr().getNextTxNum() - lastTxNum > TX_COUNT_TO_CHECKPOINT) {
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false);
				VanillaDb.txMgr().createCheckpoint(tx);
				tx.commit();
				lastTxNum = VanillaDb.txMgr().getNextTxNum();
			}
		} else if (MY_METHOD == METHOD_PERIODIC) {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			VanillaDb.txMgr().createCheckpoint(tx);
			tx.commit();
		}
	}

	@Override
	public void run() {
		while (true) {
			createCheckpoint();
			try {
				Thread.sleep(PERIOD);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
