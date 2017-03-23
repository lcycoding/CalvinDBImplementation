package org.vanilladb.comm.server;

import org.vanilladb.comm.messages.P2pMessage;

public interface ServerP2pMessageListener {
	public void onRecvServerP2pMessage(P2pMessage msg);
}
