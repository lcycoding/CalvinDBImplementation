package netdb.software.benchmark;

public class TxnResultSet {
	private TransactionType txnType;
	private long keyingTime;
	private long thinkTime;
	private long txnResponseTimeNs;
	private boolean txnIsCommited;
	private String outMsg;

	public TxnResultSet() {

	}

	public TxnResultSet(TransactionType txnType, long keyingTime,
			long thinkTime, long txnResponseTime, boolean txnIsCommited,
			String outMsg) {
		this.txnType = txnType;
		this.keyingTime = keyingTime;
		this.thinkTime = thinkTime;
		this.txnResponseTimeNs = txnResponseTime;
		this.txnIsCommited = txnIsCommited;
		this.outMsg = outMsg;
	}

	public TransactionType getTxnType() {
		return txnType;
	}

	public void setTxnType(TransactionType txnType) {
		this.txnType = txnType;
	}

	public long getKeyingTime() {
		return keyingTime;
	}

	public void setKeyingTime(long keyingTime) {
		this.keyingTime = keyingTime;
	}

	public long getThinkTime() {
		return thinkTime;
	}

	public void setThinkTime(long thinkTime) {
		this.thinkTime = thinkTime;
	}

	public long getTxnResponseTime() {
		return txnResponseTimeNs;
	}

	public void setTxnResponseTimeNs(long txnResponseTime) {
		this.txnResponseTimeNs = txnResponseTime;
	}

	public boolean isTxnIsCommited() {
		return txnIsCommited;
	}

	public void setTxnIsCommited(boolean txnIsCommited) {
		this.txnIsCommited = txnIsCommited;
	}

	public String getOutMsg() {
		return outMsg;
	}

	public void setOutMsg(String outMsg) {
		this.outMsg = outMsg;
	}

}
