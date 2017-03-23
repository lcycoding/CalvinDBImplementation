package org.vanilladb.dd.remote.groupcomm.client;

public class GcDriver {

	private int myId;

	public GcDriver(int id) {
		myId = id;
	}

	public GcConnection init() {
		return new GcConnection(myId);
	}
}
