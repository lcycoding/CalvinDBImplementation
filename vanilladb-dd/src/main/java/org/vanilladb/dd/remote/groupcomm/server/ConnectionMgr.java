package org.vanilladb.dd.remote.groupcomm.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.comm.messages.ChannelType;
import org.vanilladb.comm.messages.P2pMessage;
import org.vanilladb.comm.messages.TotalOrderMessage;
import org.vanilladb.comm.server.ServerAppl;
import org.vanilladb.comm.server.ServerNodeFailListener;
import org.vanilladb.comm.server.ServerP2pMessageListener;
import org.vanilladb.comm.server.ServerTotalOrderedMessageListener;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.cache.naive.NaiveCacheMgr;
import org.vanilladb.dd.remote.groupcomm.ClientResponse;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.remote.groupcomm.Tuple;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.VanillaDdDb.ServiceType;

public class ConnectionMgr implements ServerTotalOrderedMessageListener,
		ServerP2pMessageListener, ServerNodeFailListener {
	private static Logger logger = Logger.getLogger(ConnectionMgr.class
			.getName());

	private ServerAppl serverAppl;
	private int myId;
	private BlockingQueue<TotalOrderMessage> tomQueue = new LinkedBlockingQueue<TotalOrderMessage>();

	public ConnectionMgr(int id) {
		myId = id;
		serverAppl = new ServerAppl(id, this, this, this);
		serverAppl.start();

		// wait for all servers to start up
		if (logger.isLoggable(Level.INFO))
			logger.info("wait for all servers to start up comm. module");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		serverAppl.startPFD();
		VanillaDb.taskMgr().runTask(new Task() {

			@Override
			public void run() {
				while (true) {
					try {
						TotalOrderMessage tom = tomQueue.take();
						for (int i = 0; i < tom.getMessages().length; ++i) {
							StoredProcedureCall spc = (StoredProcedureCall) tom
									.getMessages()[i];
							spc.setTxNum(tom.getTotalOrderIdStart() + i);
							VanillaDdDb.scheduler().schedule(spc);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
		});
	}

	public void sendClientResponse(int clientId, int rteId, long txNum,
			SpResultSet rs) {
		// call the communication module to send the response back to client
		P2pMessage p2pmsg = new P2pMessage(new ClientResponse(clientId, rteId,
				txNum, rs), clientId, ChannelType.CLIENT);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	public void callStoredProc(int pid, Object... pars) {
		StoredProcedureCall[] spcs = { new StoredProcedureCall(myId, pid, pars) };
		serverAppl.sendTotalOrderRequest(spcs);
	}

	public void pushTupleSet(int nodeId, TupleSet reading) {
		P2pMessage p2pmsg = new P2pMessage(reading, nodeId, ChannelType.SERVER);
		serverAppl.sendP2pMessage(p2pmsg);
	}

	@Override
	public void onRecvServerP2pMessage(P2pMessage p2pmsg) {
		CalvinCacheMgr cm = (CalvinCacheMgr)VanillaDdDb.cacheMgr();
		Object msg = p2pmsg.getMessage();
		if (msg.getClass().equals(TupleSet.class)) {
			TupleSet ts = (TupleSet) msg;
			for (Tuple t : ts.getTupleSet()) {
				if (VanillaDdDb.serviceType() == ServiceType.CALVIN) {
					// TODO: Cache remote records
					cm.cacheInsert(t.key, t.srcTxNum, t.rec);
					
				} else
					throw new IllegalArgumentException(
							"Service Type Not Found Exception");
			}
		} else
			throw new IllegalArgumentException();
	}
	

	@Override
	public void onRecvServerTotalOrderedMessage(TotalOrderMessage tom) {
		try {
			tomQueue.put(tom);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onNodeFail(int id, ChannelType ct) {
		// do nothing
	}
}
