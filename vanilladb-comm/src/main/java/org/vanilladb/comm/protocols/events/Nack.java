package org.vanilladb.comm.protocols.events;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Session;
import net.sf.appia.core.events.SendableEvent;

public class Nack extends SendableEvent {

    public Nack() {
        super();
    }

    public Nack(Channel channel, int dir, Session source)
            throws AppiaEventException {
        super(channel, dir, source);
    }
}
