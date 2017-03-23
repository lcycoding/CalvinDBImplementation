package org.vanilladb.comm.messages;


public interface P2pMessageListener {
    public void onRecvP2pMessage(P2pMessage o);
}
