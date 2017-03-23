package netdb.software.benchmark.vanilladddb.metadata;

import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class MicroBenchPartitionMetaMgr extends PartitionMetaMgr {

	private PartitionMetaMgr partitionMetaMgr ;
	
	public MicroBenchPartitionMetaMgr() {
	}

	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	public int getPartition(RecordKey key) {
		/*
		 * Hard code the partitioning rules for Micro-benchmark testbed.
		 * Partitions each item id through mod.
		 */
		int iid = (Integer) key.getKeyVal("i_id").asJavaVal();
		return (iid - 1) / 100000;
//		return (iid - 1) % partitionMetaMgr.NUM_PARTITIONS;
	}
}
