package org.vanilladb.comm.server;

import org.vanilladb.comm.messages.TotalOrderMessage;

public interface ServerTotalOrderedMessageListener {
	public void onRecvServerTotalOrderedMessage(TotalOrderMessage msg);
}
