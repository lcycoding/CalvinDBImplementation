package org.vanilladb.dd.remote.groupcomm.client;

public class BatchGcDriver {

	private int myId;

	public BatchGcDriver(int id) {
		myId = id;
	}

	public BatchGcConnection init() {
		BatchGcConnection bc = new BatchGcConnection(myId);
		new Thread(null, bc, "batchGcConnMgr").start();
		return bc;
	}
}
