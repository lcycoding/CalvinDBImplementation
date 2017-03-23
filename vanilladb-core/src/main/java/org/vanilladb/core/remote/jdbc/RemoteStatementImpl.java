package org.vanilladb.core.remote.jdbc;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The RMI server-side implementation of RemoteStatement.
 */
@SuppressWarnings("serial")
class RemoteStatementImpl extends UnicastRemoteObject implements
		RemoteStatement {
	private RemoteConnectionImpl rconn;

	public RemoteStatementImpl(RemoteConnectionImpl rconn)
			throws RemoteException {
		this.rconn = rconn;
	}

	/**
	 * Executes the specified SQL query string. The method calls the query
	 * planner to create a plan for the query. It then sends the plan to the
	 * RemoteResultSetImpl constructor for processing.
	 * 
	 * @see RemoteStatement#executeQuery(java.lang.String)
	 */
	@Override
	public RemoteResultSet executeQuery(String qry) throws RemoteException {
		try {
			Transaction tx = rconn.getTransaction();
			Plan pln = VanillaDb.newPlanner().createQueryPlan(qry, tx);
			return new RemoteResultSetImpl(pln, rconn);
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}

	/**
	 * Executes the specified SQL update command. The method sends the command
	 * to the update planner, which executes it.
	 * 
	 * @see RemoteStatement#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String cmd) throws RemoteException {
		try {
			Transaction tx = rconn.getTransaction();
			if (tx.isReadOnly())
				throw new UnsupportedOperationException();
			int result = VanillaDb.newPlanner().executeUpdate(cmd, tx);
			if (rconn.getAutoCommit())
				rconn.commit();
			else
				rconn.endStatement();
			return result;
		} catch (RuntimeException e) {
			rconn.rollback();
			throw e;
		}
	}
}
