package org.vanilladb.dd.schedule.naive;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public abstract class NaiveStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;

	// Record keys
	private List<String> readTables = new ArrayList<String>();
	private List<String> writeTables = new ArrayList<String>();
	private List<RecordKey> readKeys = new ArrayList<RecordKey>();
	private List<RecordKey> writeKeys = new ArrayList<RecordKey>();
	private String[] readTablesForLock, writeTablesForLock;
	private RecordKey[] readKeysForLock, writeKeysForLock;
	
	public NaiveStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys();
	
	/**
	 * Perform the transaction logic and record the result of the transaction.
	 */
	protected abstract void performTransactionLogic();

	
	/**********************
	 * Implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		this.tx = VanillaDdDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// prepare keys
		prepareKeys();
	}

	public void requestConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();

		readTablesForLock = readTables.toArray(new String[0]);
		writeTablesForLock = writeTables.toArray(new String[0]);
		readKeysForLock = readKeys.toArray(new RecordKey[0]);
		writeKeysForLock = writeKeys.toArray(new RecordKey[0]);
		
		ccMgr.prepareSp(readTablesForLock, writeTablesForLock);
		ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
	}

	@Override
	public final RecordKey[] getReadSet() {
		return readKeysForLock;
	}

	@Override
	public final RecordKey[] getWriteSet() {
		return writeKeysForLock;
	}

	@Override
	public SpResultSet execute() {
		
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// Execute transaction
			performTransactionLogic();

			// The transaction finishes normally
			tx.commit();

		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}

		return paramHelper.createResultSet();
	}
	
	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
	
	protected void addReadTable(String tableName) {
		readTables.add(tableName);
	}
	
	protected void addWriteTable(String tableName) {
		writeTables.add(tableName);
	}
	
	protected void addReadKey(RecordKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		writeKeys.add(writeKey);
	}
	
	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(readTablesForLock, writeTablesForLock);
		ccMgr.executeSp(readKeysForLock, writeKeysForLock);
	}
}
