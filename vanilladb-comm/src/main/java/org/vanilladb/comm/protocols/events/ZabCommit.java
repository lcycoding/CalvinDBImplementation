package org.vanilladb.comm.protocols.events;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Session;

public class ZabCommit extends BroadcastEvent {
	 public ZabCommit() {
		 super();
	 }

    public ZabCommit(Channel channel, int direction, Session src)
            throws AppiaEventException {
        super(channel, direction, src);
    }
}
