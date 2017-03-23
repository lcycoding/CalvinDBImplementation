package org.vanilladb.dd.storage.metadata;

import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.util.DDProperties;

public abstract class PartitionMetaMgr {

	public final static int NUM_PARTITIONS;

	static {
		NUM_PARTITIONS = DDProperties.getLoader().getPropertyAsInteger(
				PartitionMetaMgr.class.getName() + ".NUM_PARTITIONS", 1);
	}
	
	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition id
	 */
	public abstract int getPartition(RecordKey key);
}
