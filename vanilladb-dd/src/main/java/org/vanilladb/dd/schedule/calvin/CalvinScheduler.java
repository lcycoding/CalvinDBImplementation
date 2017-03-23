package org.vanilladb.dd.schedule.calvin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.calvin.CalvinStoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.dd.util.DDProperties;

public class CalvinScheduler extends Task implements Scheduler {

	private static final Class<?> FACTORY_CLASS;

	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
	private int readCount;
	private int writeCount;
	private int serverId = VanillaDdDb.serverId();
	private ArrayList<Object> localReads = new ArrayList<Object>();
	private ArrayList<Object> localWrites = new ArrayList<Object>();
	private ArrayList<Object> remoteReads = new ArrayList<Object>();
	
	
	static {
		FACTORY_CLASS = DDProperties.getLoader().getPropertyAsClass(
				CalvinScheduler.class.getName() + ".FACTORY_CLASS", null,
				CalvinStoredProcedureFactory.class);
		if (FACTORY_CLASS == null)
			throw new RuntimeException("Factory property is empty");
	}

	public CalvinScheduler() {
		try {
			factory = (CalvinStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public void schedule(StoredProcedureCall... calls) {
		try {
			for (int i = 0; i < calls.length; i++) {
				spcQueue.put(calls[i]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		synchronized(this)
		{
			while (true) {
				try {
					// retrieve stored procedure call
					StoredProcedureCall call = spcQueue.take();
					if (call.isNoOpStoredProcCall())
						continue;

					// create store procedure and prepare
					DdStoredProcedure sp = factory.getStoredProcedure(
							call.getPid(), call.getTxNum());
					// prepare local param
//					Object[] newPars = readWriteSetAnalysis(call);
					sp.prepare(call.getPars());
					
					
					// log request
					if (!sp.isReadOnly())
						DdRecoveryMgr.logRequest(call);

					// create a new task for multi-thread
					CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
							call.getClientId(), call.getRteId(), call.getTxNum(),
							sp);

					// perform conservative locking
					spt.lockConservatively();

					// hand over to a thread to run the task
					VanillaDb.taskMgr().runTask(spt);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}
	

	private Object[] readWriteSetAnalysis(StoredProcedureCall call)
	{
		int indexCnt = 0;
		
		readCount = (Integer) call.getPars()[indexCnt++];
		int readItemId;
		for (int i = 0; i < readCount; i++)
		{
			readItemId = (Integer) call.getPars()[indexCnt++];
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(readItemId));
			RecordKey key = new RecordKey("item", keyEntryMap);
			
			int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
			if(partitionNum == serverId)
				localReads.add(call.getPars()[indexCnt-1]);
			else
				remoteReads.add(call.getPars()[indexCnt-1]);
		}
				
		writeCount = (Integer) call.getPars()[indexCnt++];
		int writeItemId;
		for (int i = 0; i < writeCount; i++)
		{
			writeItemId = (Integer) call.getPars()[indexCnt++];
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(writeItemId));
			RecordKey key = new RecordKey("item", keyEntryMap);
			
			int partitionNum = VanillaDdDb.partitionMetaMgr().getPartition(key);
			if(partitionNum == serverId)
				localWrites.add(call.getPars()[indexCnt-1]);
			else
				remoteReads.add(call.getPars()[indexCnt-1]);
		}
			
		ArrayList<Object> localPars = new ArrayList<Object>();
		for(Object o : localReads){
			localPars.add(o);
		}
		for(Object o : localWrites){
			localPars.add(o);
		}
		// add localWrites list thiefly
		localPars.add(localWrites);
		Object[] newPars = localPars.toArray();
		return newPars;
	}
}
