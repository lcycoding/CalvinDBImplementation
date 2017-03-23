package org.vanilladb.dd.schedule;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.dd.sql.RecordKey;

public interface DdStoredProcedure extends StoredProcedure {

	RecordKey[] getReadSet();

	RecordKey[] getWriteSet();

	boolean isReadOnly();
}
