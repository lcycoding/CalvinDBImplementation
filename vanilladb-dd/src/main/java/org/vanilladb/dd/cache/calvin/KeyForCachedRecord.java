package org.vanilladb.dd.cache.calvin;

import org.vanilladb.dd.sql.RecordKey;

public class KeyForCachedRecord{
	
	private RecordKey key;
	private long txNum;
	
	public KeyForCachedRecord(RecordKey key, long txNum){
		this.key = key;
		this.txNum = txNum;
	}
	
	public boolean equals(KeyForCachedRecord key){
		return this.key.equals(key.getRecordKey())&&(this.txNum == key.getTransactionNum());
	}
	
	public RecordKey getRecordKey(){
		return this.key;
	}
	
	public long getTransactionNum(){
		return this.txNum;
	}
	
	
	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + key.hashCode();
		return (int) (31 * hashCode + txNum);
	}
}
