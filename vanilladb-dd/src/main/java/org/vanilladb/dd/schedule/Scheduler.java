package org.vanilladb.dd.schedule;

import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;

public interface Scheduler {

	void schedule(StoredProcedureCall... calls);
}
