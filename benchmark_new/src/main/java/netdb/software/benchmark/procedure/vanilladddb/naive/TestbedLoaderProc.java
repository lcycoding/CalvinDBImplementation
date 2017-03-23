package netdb.software.benchmark.procedure.vanilladddb.naive;

import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.TpccConstants;
import netdb.software.benchmark.procedure.TestbedLoaderProcParamHelper;
import netdb.software.benchmark.util.DoublePlainPrinter;
import netdb.software.benchmark.util.RandomValueGenerator;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.dd.schedule.naive.NaiveStoredProcedure;

public class TestbedLoaderProc extends
		NaiveStoredProcedure<TestbedLoaderProcParamHelper> {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class
			.getName());

	private RandomValueGenerator rg = new RandomValueGenerator();

	public TestbedLoaderProc(long txNum) {
		super(txNum, new TestbedLoaderProcParamHelper());
	}
	
	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	protected void prepareKeys() {
		for (String tableName : paramHelper.getInsertedTableNames())
			this.addWriteTable(tableName);
	}

	@Override
	protected void performTransactionLogic() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		RecoveryMgr.logSetVal(false);

		// Generate item records
		generateItems(1, TpccConstants.NUM_ITEMS);

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		RecoveryMgr.logSetVal(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");
	}

	private void generateItems(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.fine("Start populating items from i_id=" + startIId
					+ " to i_id=" + endIId);

		int iid, iimid;
		String iname, idata;
		double iprice;
		String sql;
		for (int i = startIId, count = 1; i <= endIId; i++, count++) {
			iid = i;

			// Randomly generate values
			iimid = rg.number(TpccConstants.MIN_IM, TpccConstants.MAX_IM);
			iname = rg.randomAString(TpccConstants.MIN_I_NAME,
					TpccConstants.MAX_I_NAME);
			iprice = rg.fixedDecimalNumber(TpccConstants.MONEY_DECIMALS,
					TpccConstants.MIN_PRICE, TpccConstants.MAX_PRICE);
			idata = rg.randomAString(TpccConstants.MIN_I_DATA,
					TpccConstants.MAX_I_DATA);
			if (Math.random() < 0.1)
				idata = fillOriginal(idata);

			sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data) VALUES ("
					+ iid
					+ ", "
					+ iimid
					+ ", '"
					+ iname
					+ "', "
					+ DoublePlainPrinter.toPlainString(iprice)
					+ ", '"
					+ idata
					+ "' )";

			int result = VanillaDb.newPlanner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
			
			if (count % 10000 == 0 && logger.isLoggable(Level.FINE))
				logger.fine(count + " items have been populated");
		}

		if (logger.isLoggable(Level.FINE))
			logger.info("Populating items completed.");
	}

	private String fillOriginal(String data) {
		int originalLength = TpccConstants.ORIGINAL_STRING.length();
		int position = rg.number(0, data.length() - originalLength);
		String out = data.substring(0, position)
				+ TpccConstants.ORIGINAL_STRING
				+ data.substring(position + originalLength);
		return out;
	}
}
