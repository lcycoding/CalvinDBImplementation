package org.vanilladb.core.remote.storedprocedure;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

/**
 * The RMI server-side implementation of RemoteConnection for stored procedure
 * call interface.
 */
@SuppressWarnings("serial")
class RemoteConnectionImpl extends UnicastRemoteObject implements
		RemoteConnection {

	/**
	 * Creates a remote connection and begins a new transaction for it.
	 * 
	 * @throws RemoteException
	 */
	RemoteConnectionImpl() throws RemoteException {
		super();
	}

	@Override
	public SpResultSet callStoredProc(int pid, Object... pars)
			throws RemoteException {
		try {
			StoredProcedure sp = VanillaDb.spFactory().getStroredProcedure(pid);
			sp.prepare(pars);
			return sp.execute();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RemoteException(e.getMessage());
		}
	}
}
