package org.vanilladb.dd.remote.groupcomm;

import java.io.Serializable;

import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.sql.RecordKey;

public class Tuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -606284893049245719L;
	public RecordKey key;
	public CachedRecord rec;
	public long srcTxNum;
	public long destTxNum;

	public Tuple(RecordKey key, long srcTxNum, long destTxNum, CachedRecord rec) {
		this.key = key;
		this.rec = rec;
		this.srcTxNum = srcTxNum;
		this.destTxNum = destTxNum;
	}
}
