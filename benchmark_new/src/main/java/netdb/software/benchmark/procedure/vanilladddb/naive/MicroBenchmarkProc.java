package netdb.software.benchmark.procedure.vanilladddb.naive;

import java.util.HashMap;
import java.util.Map;

import netdb.software.benchmark.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.naive.NaiveCacheMgr;
import org.vanilladb.dd.schedule.naive.NaiveStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class MicroBenchmarkProc extends
		NaiveStoredProcedure<MicroBenchmarkProcParamHelper> {

	public MicroBenchmarkProc(long txNum) {
		super(txNum, new MicroBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		for (int i : paramHelper.getReadItemId()) {
			// create record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(i));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addReadKey(key);
		}

		// set write keys
		for (int i : paramHelper.getWriteItemId()) {
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(i));
			RecordKey key = new RecordKey("item", keyEntryMap);
			addWriteKey(key);
		}
	}

	@Override
	protected void performTransactionLogic() {
		NaiveCacheMgr cm = (NaiveCacheMgr) VanillaDdDb.cacheMgr();

		// SELECT i_name, i_price FROM item WHERE i_id = ...
		for (int i : paramHelper.getReadItemId()) {
			// Create a record key for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(i));
			RecordKey key = new RecordKey("item", keyEntryMap);
			
			// Read the record
			CachedRecord rec = cm.read(key, tx);
			rec.getVal("i_name");
			rec.getVal("i_price");
		}

		// UPDATE item SET i_price = ...  WHERE i_id = ...
		int[] writeItemIds = paramHelper.getWriteItemId();
		double[] newItemPrices = paramHelper.getNewItemPrice();
		for (int i = 0; i < writeItemIds.length; i++) {
			// Create a record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(writeItemIds[i]));
			RecordKey key = new RecordKey("item", keyEntryMap);

			// Create key-value pairs for writing
			CachedRecord rec = new CachedRecord();
			rec.setVal("i_price", new DoubleConstant(newItemPrices[i]));
			
			// Update the record
			cm.update(key, rec, tx);
		}
	}
}
