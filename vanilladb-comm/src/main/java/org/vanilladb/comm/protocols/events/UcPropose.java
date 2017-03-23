package org.vanilladb.comm.protocols.events;

import org.vanilladb.comm.protocols.consensusUtils.PaxosProposal;

import net.sf.appia.core.AppiaEventException;
import net.sf.appia.core.Channel;
import net.sf.appia.core.Event;
import net.sf.appia.core.Session;

public class UcPropose extends Event {
    /**
     * The value proposed for consensus.
     */
    public PaxosProposal value;

    public UcPropose() {
        super();
    }

    public UcPropose(PaxosProposal value) {
        super();
        this.value = value;
    }

    public UcPropose(Channel channel, int direction, Session src)
            throws AppiaEventException {
        super(channel, direction, src);
    }
}
