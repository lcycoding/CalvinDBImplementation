package org.vanilladb.comm.protocols.events;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Session;
import net.sf.appia.core.events.SendableEvent;

/**
 * Used by Best Effort Broadcast Protocol
 * 
 * @author mkliao
 * 
 */
public class BroadcastEvent extends SendableEvent {

	public BroadcastEvent() {
		super();
	}

	public BroadcastEvent(Channel channel, int dir, Session source)
			throws AppiaEventException {
		super(channel, dir, source);
	}

}
