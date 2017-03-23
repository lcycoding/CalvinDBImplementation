package org.vanilladb.comm.messages;

import net.sf.appia.core.Channel;

/**
 * Defines a class that receives a failed node.
 * 
 * @author mkliao
 * 
 */
public interface NodeFailListener {

	/**
	 * 
	 * @param id
	 *            the failed node id
	 * @param group
	 *            the group of the failed node
	 * @param channel
	 *            the channel the crash event delivered from
	 */
	public void onNodeFail(int id, ChannelType channelType, Channel c);
}
