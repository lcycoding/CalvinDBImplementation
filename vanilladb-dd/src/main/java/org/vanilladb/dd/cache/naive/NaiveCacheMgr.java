package org.vanilladb.dd.cache.naive;

import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.sql.RecordKey;

public class NaiveCacheMgr implements CacheMgr {

	public CachedRecord read(RecordKey key, Transaction tx) {
		return LocalRecordMgr.read(key, tx);
	}

	public void update(RecordKey key, CachedRecord rec, Transaction tx) {
		LocalRecordMgr.update(key, rec, tx);
	}

	public void insert(RecordKey key, CachedRecord rec, Transaction tx) {
		LocalRecordMgr.insert(key, rec, tx);
	}

	public void delete(RecordKey key, Transaction tx) {
		LocalRecordMgr.delete(key, tx);
	}
}
