package org.vanilladb.core.sql.storedprocedure;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

public class SpResultRecord implements Record, Serializable {

	private static final long serialVersionUID = 245365697121L;

	private transient Map<String, Constant> fldValueMap = new HashMap<String, Constant>();

	public SpResultRecord() {
	}

	@Override
	public Constant getVal(String fldName) {
		return fldValueMap.get(fldName);
	}

	public void setVal(String fldName, Constant val) {
		fldValueMap.put(fldName, val);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		Set<String> flds = new TreeSet<String>(fldValueMap.keySet());
		for (String fld : flds)
			sb.append(fld).append("=").append(fldValueMap.get(fld))
					.append(", ");
		int end = sb.length();
		sb.replace(end - 2, end, "] ");
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(SpResultRecord.class)))
			return false;
		SpResultRecord s = (SpResultRecord) obj;
		return toString().equals(s.toString());
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/**
	 * Serialize this {@code SpResultRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		Set<String> fldsSet = fldValueMap.keySet();
		out.defaultWriteObject();
		out.writeInt(fldsSet.size());

		// Write out all elements in the proper order
		for (String fld : fldsSet) {
			Constant val = fldValueMap.get(fld);
			if (val.asJavaVal() == null)
				val = Constant.defaultInstance(val.getType());
			byte[] bytes = val.asBytes();
			out.writeObject(fld);
			out.writeInt(val.getType().getSqlType());
			out.writeInt(bytes.length);
			out.write(bytes);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		fldValueMap = new HashMap<String, Constant>();
		int numFlds = in.readInt();

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType),
					bytes);
			fldValueMap.put(fld, val);
		}

	}
}
