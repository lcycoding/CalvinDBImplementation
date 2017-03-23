package org.vanilladb.dd.storage.tx.recovery;

import org.vanilladb.core.storage.tx.recovery.LogRecord;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.storage.log.DdLogMgr;

public interface DdLogRecord extends LogRecord {
	/**
	 * @see LogRecord#op()
	 */
	static final int OP_SP_REQUEST = -99;
	static DdLogMgr ddLogMgr = VanillaDdDb.DdLogMgr();

}
