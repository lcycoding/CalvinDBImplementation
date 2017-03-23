package org.vanilladb.comm.messages;

public interface TotalOrderedMessageListener {
	public void onRecvTotalOrderedMessage(TotalOrderMessage o);
}
