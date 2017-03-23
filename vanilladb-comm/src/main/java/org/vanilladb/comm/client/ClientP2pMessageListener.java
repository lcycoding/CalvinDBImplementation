package org.vanilladb.comm.client;

import org.vanilladb.comm.messages.P2pMessage;

public interface ClientP2pMessageListener {
	public void onRecvClientP2pMessage(P2pMessage msg);
}
