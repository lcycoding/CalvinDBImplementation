package org.vanilladb.dd.sql;

import org.vanilladb.core.sql.Constant;

public class RecordKeyEntry {
	private String fldName;
	private Constant value;

	public RecordKeyEntry(String fldName, Constant value) {
		this.fldName = fldName;
		this.value = value;
	}

	public String getFldName() {
		return fldName;
	}

	public Constant getValue() {
		return value;
	}

	@Override
	public String toString() {
		return fldName + "=" + value.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		RecordKeyEntry k = (RecordKeyEntry) obj;
		return k.fldName.equals(fldName) && k.value.equals(value);
	}

	@Override
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + fldName.hashCode();
		hash = hash * 31 + value.hashCode();
		return hash;
	}
}
