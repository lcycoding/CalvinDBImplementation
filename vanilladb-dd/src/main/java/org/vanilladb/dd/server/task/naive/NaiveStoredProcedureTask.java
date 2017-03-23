package org.vanilladb.dd.server.task.naive;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.naive.NaiveStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;

public class NaiveStoredProcedureTask extends StoredProcedureTask {
	
	private NaiveStoredProcedure<?> nsp;
	
	public NaiveStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
		
		nsp = (NaiveStoredProcedure<?>) sp;
	}

	public void run() {
		SpResultSet rs = sp.execute();
		VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum, rs);
	}
	
	public void lockConservatively() {
		nsp.requestConservativeLocks();
	}
}
