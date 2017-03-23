package org.vanilladb.dd.remote.groupcomm;

import java.io.Serializable;

/**
 * 
 * This class defines a stored procedure call.
 * 
 */
public class StoredProcedureCall implements Serializable {

	public static int PID_NO_OPERATION = Integer.MIN_VALUE;

	private static final long serialVersionUID = 8807383803517134106L;

	private Object[] objs;

	private long txNum = -1;

	private int clientId, pid = PID_NO_OPERATION, rteId = -1;

	public static StoredProcedureCall getNoOpStoredProcCall(int clienId) {
		return new StoredProcedureCall(clienId);
	}

	StoredProcedureCall(int clienId) {
		this.clientId = clienId;
	}

	public StoredProcedureCall(int clienId, int pid, Object... objs) {
		this.clientId = clienId;
		this.pid = pid;
		this.objs = objs;
	}

	public StoredProcedureCall(int clienId, int rteid, int pid, Object... objs) {
		this.clientId = clienId;
		this.rteId = rteid;
		this.pid = pid;
		this.objs = objs;
	}

	public Object[] getPars() {
		return objs;
	}

	public long getTxNum() {
		return txNum;
	}

	public void setTxNum(long txNum) {
		this.txNum = txNum;
	}

	public int getClientId() {
		return clientId;
	}

	public int getRteId() {
		return rteId;
	}

	public int getPid() {
		return pid;
	}

	public void setClientId(int clientId) {
		this.clientId = clientId;
	}

	public boolean isNoOpStoredProcCall() {
		return pid == PID_NO_OPERATION;
	}
}
