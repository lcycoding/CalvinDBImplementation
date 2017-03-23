package org.vanilladb.core.storage.metadata.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;

public class StatisticsRefreshTask extends Task {
	private static Logger logger = Logger.getLogger(StatisticsRefreshTask.class
			.getName());

	private List<String> refreshtbls;
	private Transaction tx;

	public StatisticsRefreshTask(Transaction tx, String... tblNames) {
		this.tx = tx;

		this.refreshtbls = new ArrayList<String>();
		for (int i = 0; i < tblNames.length; i++)
			this.refreshtbls.add(tblNames[i]);
	}

	@Override
	public void run() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start refreshing statistics of table");
		while (!refreshtbls.isEmpty())
			VanillaDb.statMgr().refreshStatistics(refreshtbls.remove(0),
					this.tx);
	}
}
