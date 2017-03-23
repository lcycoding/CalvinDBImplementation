package netdb.software.benchmark.procedure.vanilladb;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import netdb.software.benchmark.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class MicroBenchmarkProc implements StoredProcedure {

	private MicroBenchmarkProcParamHelper paramHelper = new MicroBenchmarkProcParamHelper();
	private Transaction tx;
	
	private static Object globalLock = new Object();
	
	private static Transaction prepareTransaction(RecordKey[] readKeys, RecordKey[] writeKeys) {
		Transaction tx = null;
		
		synchronized (globalLock) {
			tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
			ccMgr.prepareSp(readKeys, writeKeys);
		}
		
		return tx;
	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		tx = prepareTransaction(getReadKeys(), getWriteKeys());
		try {
			// Conservatively locking
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
			ccMgr.executeSp(getReadKeys(), getWriteKeys());
			
			executeSql();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}
		return paramHelper.createResultSet();
	}

	protected void executeSql() {
		// select the items
		for (int i = 0; i < paramHelper.getReadCount(); i++) {

			String name = "";
			double price = 0;

			String sql = "SELECT i_name, i_price FROM item WHERE i_id = "
					+ paramHelper.getReadItemId()[i];
			Plan p = VanillaDb.newPlanner().createQueryPlan(sql, tx);
			Scan s = p.open();
			s.beforeFirst();
			if (s.next()) {
				name = (String) s.getVal("i_name").asJavaVal();
				price = (Double) s.getVal("i_price").asJavaVal();
			} else
				throw new RuntimeException();
			s.close();
		}

		// update the items
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			String sql = "UPDATE item SET i_price = "
					+ paramHelper.getNewItemPrice()[i] + " WHERE i_id = "
					+ paramHelper.getWriteItemId()[i];
			int result = VanillaDb.newPlanner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

	public RecordKey[] getReadKeys() {
		int[] readItemId = paramHelper.getReadItemId();
		RecordKey[] readKeys = new RecordKey[readItemId.length];

		for (int i = 0; i < readKeys.length; i++) {
			// create record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(readItemId[i]));
			readKeys[i] = new RecordKey("item", keyEntryMap);
		}

		return readKeys;
	}

	public RecordKey[] getWriteKeys() {
		int[] writeItemId = paramHelper.getWriteItemId();
		RecordKey[] writeKeys = new RecordKey[writeItemId.length];

		for (int i = 0; i < writeKeys.length; i++) {
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(writeItemId[i]));
			writeKeys[i] = new RecordKey("item", keyEntryMap);
		}

		return writeKeys;
	}
}
