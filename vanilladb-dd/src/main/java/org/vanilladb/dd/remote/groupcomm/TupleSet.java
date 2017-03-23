package org.vanilladb.dd.remote.groupcomm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.sql.RecordKey;

public class TupleSet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3191495851408477607L;
	private List<Tuple> tuples;
	private int sinkId;

	public TupleSet(int sinkId) {
		this.tuples = new ArrayList<Tuple>();
		this.sinkId = sinkId;
	}

	public List<Tuple> getTupleSet() {
		return tuples;
	}

	public void addTuple(RecordKey key, long srcTxNum, long destTxNum,
			CachedRecord rec) {
		tuples.add(new Tuple(key, srcTxNum, destTxNum, rec));
	}

	public int sinkId() {
		return sinkId;
	}
}