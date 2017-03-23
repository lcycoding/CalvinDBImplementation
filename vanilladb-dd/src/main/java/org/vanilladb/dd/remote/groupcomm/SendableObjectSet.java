package org.vanilladb.dd.remote.groupcomm;

import java.io.Serializable;

public class SendableObjectSet implements Serializable {

	private static final long serialVersionUID = 1L;

	private long txNum;

	private int nodeId;

	private Object[] readings;

	public SendableObjectSet(int nodeId, long txNum, Object... readings) {
		this.txNum = txNum;
		this.nodeId = nodeId;
		this.readings = readings;
	}

	public long getTxNum() {
		return txNum;
	}

	public Object[] getReadSet() {
		return readings;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
}