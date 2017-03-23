package org.vanilladb.dd.server.task.calvin;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;

public class CalvinStoredProcedureTask extends StoredProcedureTask {

private CalvinStoredProcedure<?> nsp;
	
	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
		
		nsp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		SpResultSet rs = sp.execute();
		
		if(nsp.getMaster() == VanillaDdDb.serverId())
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum, rs);
	}
	
	public void lockConservatively() {
		nsp.requestConservativeLocks();
	}
}
