package org.vanilladb.core.remote.storedprocedure;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote connection interface for the of stored procedure call.
 */
public interface RemoteConnection extends Remote {

	SpResultSet callStoredProc(int pid, Object... pars) throws RemoteException;

}
