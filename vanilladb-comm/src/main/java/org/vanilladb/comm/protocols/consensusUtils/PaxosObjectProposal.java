package org.vanilladb.comm.protocols.consensusUtils;

public class PaxosObjectProposal extends PaxosProposal {
	private static final long serialVersionUID = -2249937284480902746L;
	public boolean abort;

	public Object obj;

	public PaxosObjectProposal(Object obj) {
		this.obj = obj;
	}
}
