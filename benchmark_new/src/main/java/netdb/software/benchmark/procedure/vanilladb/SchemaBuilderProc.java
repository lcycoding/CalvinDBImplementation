package netdb.software.benchmark.procedure.vanilladb;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.procedure.SchemaBuilderProcParamHelper;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class SchemaBuilderProc implements StoredProcedure {
	private static Logger logger = Logger.getLogger(SchemaBuilderProc.class
			.getName());

	private SchemaBuilderProcParamHelper paramHelper = new SchemaBuilderProcParamHelper();

	public SchemaBuilderProc() {

	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpcc testbed...");
		createSchema();
		return paramHelper.createResultSet();
	}

	private void createSchema() {
		Transaction tx = VanillaDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		paramHelper.setCommitted(true);
		try {
			for (String cmd : paramHelper.getTableSchemas())
				VanillaDb.newPlanner().executeUpdate(cmd, tx);
			for (String cmd : paramHelper.getIndexSchemas())
				VanillaDb.newPlanner().executeUpdate(cmd, tx);

			tx.commit();
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		}
	}
}
