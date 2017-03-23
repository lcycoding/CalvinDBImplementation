package org.vanilladb.dd.remote.groupcomm.client;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.vanilladb.comm.client.ClientAppl;
import org.vanilladb.comm.client.ClientNodeFailListener;
import org.vanilladb.comm.client.ClientP2pMessageListener;
import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.remote.groupcomm.ClientResponse;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;

// XXX need fix if used
public class GcConnection implements ClientP2pMessageListener,
		ClientNodeFailListener {
	private long currentTxNumStart;
	private Set<Long> currentTxNums;
	private int myId;
	private ClientAppl clientAppl;
	private Queue<ClientResponse> respQueue = new LinkedList<ClientResponse>();

	public GcConnection(int id) {
		ClientAppl clientAppl = new ClientAppl(id, this, this);
		this.clientAppl = clientAppl;
		myId = id;
		clientAppl.start();
		currentTxNums = new HashSet<Long>();
		// wait for all servers to start up
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		clientAppl.startPFD();
	}

	// TODO: batch requests and send NOP call

	public synchronized SpResultSet callStoredProc(int pid, Object... pars) {
		StoredProcedureCall spc = new StoredProcedureCall(myId, pid, pars);
		StoredProcedureCall[] spcs = { spc };
		currentTxNumStart = -1;
		clientAppl.sendRequest(spcs);
		currentTxNums.clear();
		while (true) {
			try {
				this.wait();
				while (respQueue.size() > 0) {
					ClientResponse r = respQueue.poll();
					if (currentTxNums.remove(r.getTxNum())
							&& currentTxNums.isEmpty()) {
						return (SpResultSet) r.getResultSet();
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public synchronized SpResultSet[] callBatchedStoredProc(int[] pids,
			List<Object[]> pars) {
		// StoredProcedureCall[]
		if (pids.length != pars.size())
			throw new IllegalArgumentException(
					"the length of pids and paramter list should be the same");
		// System.out.println("call batch reqeust ..");

		SpResultSet[] responses = new SpResultSet[pids.length];
		StoredProcedureCall[] spcs = new StoredProcedureCall[pids.length];
		for (int i = 0; i < pids.length; ++i)
			spcs[i] = new StoredProcedureCall(myId, pids[i], pars.get(i));

		currentTxNumStart = -1;
		clientAppl.sendRequest(spcs);
		currentTxNums.clear();
		while (true) {
			try {
				this.wait();
				while (respQueue.size() > 0) {
					ClientResponse r = respQueue.poll();
					if (currentTxNums.remove(r.getTxNum())) {
						// System.out.println("get resp tx:" + r.getTxNum());
						responses[(int) (r.getTxNum() - currentTxNumStart)] = (SpResultSet) r
								.getResultSet();
						if (currentTxNums.isEmpty())
							return responses;
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public synchronized void onRecvClientP2pMessage(P2pMessage p2pmsg) {
		// notify the client thread to check the response
		ClientResponse c = (ClientResponse) p2pmsg.getMessage();
		if (c.getClientId() == myId && currentTxNums.contains(c.getTxNum())) {
			respQueue.add(c);
			notifyAll();
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType ct) {
		// do nothing
	}
}
