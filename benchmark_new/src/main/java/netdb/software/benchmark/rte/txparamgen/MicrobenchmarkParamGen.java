package netdb.software.benchmark.rte.txparamgen;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.TpccConstants;
import netdb.software.benchmark.TransactionType;
import netdb.software.benchmark.util.RandomNonRepeatGenerator;
import netdb.software.benchmark.util.RandomValueGenerator;

import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class MicrobenchmarkParamGen implements TxParamGenerator {

	private static final double REMOTE_RATE;
	private static final double WRITE_PERCENTAGE;
	private static final double CONFLICT_RATE;
	private static final int REMOTE_HOT_COUNT = 3;
	private static final int REMOTE_COLD_COUNT = 3;

	private static final int PARTITION_NUM;
	private static final int DATA_SIZE_PER_PART;
	private static final int HOT_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_PER_TX = 9;
	private static final int HOT_DATA_PER_TX = 1;

	private static final int DATA_PER_USER;

	private static Map<Integer, Integer> itemRandomMap;
	static {

		String prop = null;

		prop = System.getProperty(MicrobenchmarkParamGen.class.getName()
				+ ".CONFLICT_RATE");
		CONFLICT_RATE = (prop == null ? 0.01 : Double.parseDouble(prop.trim()));

		prop = System.getProperty(MicrobenchmarkParamGen.class.getName()
				+ ".REMOTE_RATE");
		REMOTE_RATE = (prop == null ? 0.2 : Double.parseDouble(prop.trim()));

		prop = System.getProperty(MicrobenchmarkParamGen.class.getName()
				+ ".WRITE_PERCENTAGE");
		WRITE_PERCENTAGE = (prop == null ? 0.0 : Double
				.parseDouble(prop.trim()));
		
		PARTITION_NUM = PartitionMetaMgr.NUM_PARTITIONS;
		DATA_SIZE_PER_PART = TpccConstants.NUM_ITEMS / PARTITION_NUM;
		HOT_DATA_SIZE_PER_PART = (int) (1.0 / CONFLICT_RATE);
		COLD_DATA_SIZE_PER_PART = DATA_SIZE_PER_PART - HOT_DATA_SIZE_PER_PART;
		DATA_PER_USER = COLD_DATA_SIZE_PER_PART; // XXX quick fix to disable
													// user session

		// initialize random item mapping map
		itemRandomMap = new HashMap<Integer, Integer>(TpccConstants.NUM_ITEMS);

		for (int i = 1; i <= TpccConstants.NUM_ITEMS; i++)
			itemRandomMap.put(i, i);
	}

	private Object[] params;
	private int sessionUser = 0;

	public MicrobenchmarkParamGen() {

	}

	@Override
	public TransactionType getTxnType() {
		return TransactionType.MICROBENCHMARK_TXN;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		LinkedList<Object> paramList = new LinkedList<Object>();

		// decide there is remote access or not
		boolean isRemote = (rvg.randomChooseFromDistribution(REMOTE_RATE,
				(1 - REMOTE_RATE)) == 0) ? true : false;

		// decide there is write or not
		boolean isWrite = (rvg.randomChooseFromDistribution(WRITE_PERCENTAGE,
				(1 - WRITE_PERCENTAGE)) == 0) ? true : false;

		// **********************
		// Start prepare params
		// **********************

		// randomly choose the main partition
		int mainPartition = rvg.number(0, PARTITION_NUM - 1);
		
		// set read count
		int local_hot_count = HOT_DATA_PER_TX;
		int local_cold_count = COLD_DATA_PER_TX;
		int remote_hot_count = REMOTE_HOT_COUNT;
		int remote_cold_count = REMOTE_COLD_COUNT;

		int totalReadCount = local_cold_count + local_hot_count
				+ (isRemote ? (remote_hot_count + remote_cold_count) : 0);

		paramList.add(totalReadCount);

		// randomly choose a hot data
		chooseHotData(paramList, mainPartition, local_hot_count);

		// randomly choose COLD_DATA_PER_TX data from cold dataset
		chooseColdData(paramList, mainPartition, local_cold_count);

		// remote
		if (isRemote) {

			// randomly choose hot data from other partitions
			int[] partitionHotCount = new int[PARTITION_NUM];
			partitionHotCount[mainPartition] = 0;
			for (int i = 0; i < remote_hot_count; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition,
						rvg);
				partitionHotCount[remotePartition]++;
			}

			for (int i = 0; i < PARTITION_NUM; i++)
				chooseHotData(paramList, i, partitionHotCount[i]);

			// randomly choose cold data from other partitions
			int[] partitionColdCount = new int[PARTITION_NUM];
			partitionColdCount[mainPartition] = 0;

			for (int i = 0; i < remote_cold_count; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition,
						rvg);
				partitionColdCount[remotePartition]++;
			}

			for (int i = 0; i < PARTITION_NUM; i++)
				chooseColdData(paramList, i, partitionColdCount[i]);
		}

		// write
		if (isWrite) {

			totalReadCount = paramList.size() - 1;

			// set write count = read count
			paramList.add((Integer) paramList.get(0));

			// for each item been read, set their item id to be written
			for (int i = 0; i < totalReadCount; i++)
				paramList.add(paramList.get(i + 1));

			// set the update value
			for (int i = 0; i < totalReadCount; i++)
				paramList.add(rvg.nextDouble() * 100000);

		} else {
			// set write count to 0
			paramList.add(0);
		}

		params = paramList.toArray();
		return params;
	}
	
	private int randomChooseOtherPartition(int mainPartition,
			RandomValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, PARTITION_NUM - 1)) % PARTITION_NUM);
	}

	private void chooseHotData(List<Object> paramList, int partition, int count) {
		int minMainPart = partition * DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(
				HOT_DATA_SIZE_PER_PART);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPart + tmp;
			itemId = itemRandomMap.get(itemId);
			paramList.add(itemId);
		}

	}

	private void chooseColdData(List<Object> paramList, int partition, int count) {
		int minMainPartColdData = partition * DATA_SIZE_PER_PART
				+ HOT_DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(
				DATA_PER_USER);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPartColdData + sessionUser * DATA_PER_USER
					+ tmp;

			itemId = itemRandomMap.get(itemId);
			paramList.add(itemId);
		}
	}
}
