package org.vanilladb.dd.storage.metadata;

import org.vanilladb.dd.sql.RecordKey;

public class HashBasedPartitionMetaMgr extends PartitionMetaMgr {

	/**
	 * Decides the partition of each record.
	 * 
	 * @param key
	 * @return the partition id
	 */
	@Override
	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules.
		 */
		return key.hashCode() % NUM_PARTITIONS;
	}
}
