package org.vanilladb.dd.cache.calvin;

import java.awt.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.sql.RecordKey;

public class CalvinCacheMgr implements CacheMgr {
	
	
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
	
	private ConcurrentHashMap<KeyForCachedRecord, CachedRecord> remoteCache = new ConcurrentHashMap<KeyForCachedRecord, CachedRecord>();

	public void cacheInsert(RecordKey key, long txNum, CachedRecord rec) {
		synchronized(getAnchor(key)){
			KeyForCachedRecord cacheKey = new KeyForCachedRecord(key, txNum);
			remoteCache.put(cacheKey, rec);
			getAnchor(key).notifyAll();
		}
	}
	
	public CachedRecord cacheRead(RecordKey key, long txNum) {
		synchronized(getAnchor(key)){
			KeyForCachedRecord cacheKey = new KeyForCachedRecord(key, txNum);
			while(remoteCache.get(cacheKey) == null){
				try {
					getAnchor(key).wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			return remoteCache.get(key);
		}
	}
	
	public void cacheClear(){
		remoteCache.clear();
	}
	
	private Object[] anchors = new Object[100];
	
	boolean firstInit = true;
	private Object getAnchor(RecordKey key){
		if(firstInit == true){
			initAnchor();
			firstInit = false;
		}
		
		return anchors[key.hashCode() % anchors.length];
	}
	
	private void initAnchor(){
		for(int i=0; i<anchors.length; i++){
			anchors[i] = new Object();
		}
	}
	
}
