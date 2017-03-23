package org.vanilladb.comm.protocols.events;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Event;
import net.sf.appia.core.Session;

public class ZabRequest extends Event {
	private Object obj;

    public ZabRequest(Channel channel, int direction, Session src, Object o)
            throws AppiaEventException {
        super(channel, direction, src);
		this.obj = o;
    }
	
	public Object getObject() {
		return obj;
	}
}
