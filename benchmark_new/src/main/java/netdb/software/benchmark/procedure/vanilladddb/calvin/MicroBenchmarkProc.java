package netdb.software.benchmark.procedure.vanilladddb.calvin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.Tuple;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class MicroBenchmarkProc extends
CalvinStoredProcedure<MicroBenchmarkProcParamHelper>{

	// forwarding handling
		private ConnectionMgr connectionMgr = (ConnectionMgr) VanillaDdDb.connectionMgr();
		
		private int serverId = VanillaDdDb.serverId();
		private List<RecordKey> remoteReadList = new ArrayList<RecordKey>();
		private HashMap<Integer,ArrayList<RecordKey>> activeParticipant = new HashMap<Integer,ArrayList<RecordKey>>();
		
		
		public MicroBenchmarkProc(long txNum) {
			super(txNum, new MicroBenchmarkProcParamHelper());
		}
		
		@Override
		public void prepareKeys() {
			boolean first = true;
			// set read keys
			for (int i : paramHelper.getReadItemId()) {
				// create record key for reading
				Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("i_id", new IntegerConstant(i));
				RecordKey key = new RecordKey("item", keyEntryMap);
				
				int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
				if(partitionNum == serverId)
					addReadKey(key);
				else
					remoteReadList.add(key);
				
				
				if(paramHelper.getWriteItemId().length == 0){
					// choose master
					if(first == true){
						this.setMaster(partitionNum);
						first = false;
					}
				}
			}
			

			// set write keys
			for (int i : paramHelper.getWriteItemId()) {
					
				// create record key for writing
				Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("i_id", new IntegerConstant(i));
				RecordKey key = new RecordKey("item", keyEntryMap);
				
				int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
				if(partitionNum == serverId)
					addWriteKey(key);
				/// don't care remote write
				
				/// add active list
				if(!activeParticipant.containsKey(partitionNum)){
					activeParticipant.put(partitionNum, new ArrayList<RecordKey>());
				}
				activeParticipant.get(partitionNum).add(key);
				
				// choose master
				if(first == true){
					this.setMaster(partitionNum);
					first = false;
				}
			}
		}
		
		@Override
		protected void performTransactionLogic() {
			CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
			
			// SELECT i_name, i_price FROM item WHERE i_id = ...
			for ( int i : paramHelper.getReadItemId()) {
				// Create a record key for reading
				Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("i_id", new IntegerConstant(i));
				RecordKey key = new RecordKey("item", keyEntryMap);
				
				int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
				if(partitionNum == serverId) {
					// Read the record
					CachedRecord rec = cm.read(key, tx);
					rec.getVal("i_name");
					rec.getVal("i_price");
				}
			}
			
			// handle forwarding
			forwardingReadResult();
			
			if(this.getWriteSet().length != 0){
				// get forwarding result
				
				for(RecordKey key : remoteReadList) {
					cm.cacheRead(key, tx.getTransactionNumber());
				}
			}
				 			
			
			// UPDATE item SET i_price = ...  WHERE i_id = ...
			int[] writeItemIds = paramHelper.getWriteItemId();
			double[] newItemPrices = paramHelper.getNewItemPrice();
			for (int i = 0; i < writeItemIds.length; i++) {
				// Create a record key for writing
				Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("i_id", new IntegerConstant(writeItemIds[i]));
				RecordKey key = new RecordKey("item", keyEntryMap);

				int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
				if(partitionNum == serverId){
					// Create key-value pairs for writing
					CachedRecord rec = new CachedRecord();
					rec.setVal("i_price", new DoubleConstant(newItemPrices[i]));
					
					// Update the record
					cm.update(key, rec, tx);
				}
			}
			
			// remember to clear the cache
		}
		
		private void forwardingReadResult()
		{
			CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
			TupleSet tupleSet = new TupleSet(VanillaDdDb.serverId());
			
			// prepare tupleSet
			for(RecordKey key : this.getReadSet())
			{
				CachedRecord rec = cm.read(key, tx);
				tupleSet.addTuple(key, tx.getTransactionNumber(), tx.getTransactionNumber(), rec);
			}
			
			if(paramHelper.getWriteItemId().length == 0){
				connectionMgr.pushTupleSet(this.getMaster(), tupleSet);
			} 
			else{
				// each list for one server to send
				for(int nodeId : activeParticipant.keySet())
				{
					connectionMgr.pushTupleSet(nodeId, tupleSet);
				}
			}	
		}
}
