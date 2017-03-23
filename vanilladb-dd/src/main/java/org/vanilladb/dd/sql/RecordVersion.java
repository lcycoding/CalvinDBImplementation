package org.vanilladb.dd.sql;

public class RecordVersion {
	public RecordKey key;
	public long srcTxNum;

	public RecordVersion(RecordKey key, long srcTxNum) {
		this.key = key;
		this.srcTxNum = srcTxNum;
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + key.hashCode();
		hashCode = 31 * hashCode
				+ (int) (this.srcTxNum ^ (this.srcTxNum >>> 32));
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != RecordVersion.class)
			return false;
		RecordVersion rv = (RecordVersion) obj;
		return rv.key.equals(this.key) && rv.srcTxNum == this.srcTxNum;
	}

	@Override
	public String toString() {
		return srcTxNum + "#" + key.toString();
	}
}
