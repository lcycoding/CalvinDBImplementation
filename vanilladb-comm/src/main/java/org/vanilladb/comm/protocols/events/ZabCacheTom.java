package org.vanilladb.comm.protocols.events;

import org.vanilladb.comm.messages.TotalOrderMessage;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Event;
import net.sf.appia.core.Session;

public class ZabCacheTom extends Event {
	
	private TotalOrderMessage tom;
	
	public TotalOrderMessage getTom(){
		return tom;
	}

	public ZabCacheTom(Channel channel, int direction, Session src, TotalOrderMessage tom)
			throws AppiaEventException {
		super(channel, direction, src);
		this.tom = tom;
	}
}
