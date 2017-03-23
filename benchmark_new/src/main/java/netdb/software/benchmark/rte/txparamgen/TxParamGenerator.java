package netdb.software.benchmark.rte.txparamgen;

import netdb.software.benchmark.TransactionType;

public interface TxParamGenerator {

	TransactionType getTxnType();

	Object[] generateParameter();

}
