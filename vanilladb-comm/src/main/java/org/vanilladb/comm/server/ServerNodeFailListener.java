package org.vanilladb.comm.server;

import org.vanilladb.comm.messages.ChannelType;
/**
 * Defines a class that receives a failed node.
 * 
 * @author mkliao
 * 
 */
public interface ServerNodeFailListener {

	/**
	 * 
	 * @param id
	 *            the failed node id
	 * @param group
	 *            the group of the failed node
	 * @param channel
	 *            the channel the crash event delivered from
	 */
	public void onNodeFail(int id, ChannelType channelType);
}
