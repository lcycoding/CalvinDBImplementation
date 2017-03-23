package org.vanilladb.comm.protocols.events;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Session;

/**
 * Used by Eager Reliable Broadcast Protocol
 * 
 * @author mkliao
 * 
 */
public class ReliableBroadcastEvent extends BroadcastEvent {

	public ReliableBroadcastEvent() {
		super();
	}

	public ReliableBroadcastEvent(Channel channel, int dir, Session source)
			throws AppiaEventException {
		super(channel, dir, source);
	}

}
