package org.vanilladb.comm.messages;

import java.io.Serializable;

/**
 * 
 * @author mkliao
 *
 */
public class TotalOrderMessage implements Serializable {

	private static final long serialVersionUID = 8807383803517134106L;
	private long totalOrderSequenceNumber;
	private Object[] messages;
	private long totalOrderIdStart;

	public TotalOrderMessage(Object[] msgs) {
		this.messages = msgs;
		this.totalOrderIdStart = -1;
	}

	public Object[] getMessages() {
		return messages;
	}

	public long getTotalOrderIdStart() {
		return totalOrderIdStart;
	}

	public void setTotalOrderIdStart(long totalOrderId) {
		this.totalOrderIdStart = totalOrderId;
	}

	public long getTotalOrderSequenceNumber(){
		return totalOrderSequenceNumber;
	}
	
	public void setTotalOrderSequenceNumber(long tosn){
		this.totalOrderSequenceNumber = tosn;
	}
}
