package org.vanilladb.comm.client;

import org.vanilladb.comm.messages.ChannelType;

/**
 * Defines a class that receives a failed node.
 * 
 * @author mkliao
 * 
 */
public interface ClientNodeFailListener {

	/**
	 * 
	 * @param id
	 *            the failed node id
	 * @param group
	 *            the group of the failed node
	 */
	public void onNodeFail(int id, ChannelType channelType);
}
