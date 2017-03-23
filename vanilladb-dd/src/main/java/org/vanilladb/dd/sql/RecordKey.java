package org.vanilladb.dd.sql;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.predicate.ConstantExpression;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;

public class RecordKey implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -958208157248894658L;
	private String tableName;
	private transient Map<String, Constant> keyEntryMap;

	public RecordKey(String tableName, Map<String, Constant> keyEntryMap) {
		this.tableName = tableName;
		this.keyEntryMap = keyEntryMap;
	}

	public RecordKey(String tableName, String[] flds, Constant[] vals) {
		this.tableName = tableName;
		if (flds.length != vals.length)
			throw new IllegalArgumentException();
		HashMap<String, Constant> map = new HashMap<String, Constant>();
		for (int i = 0; i < flds.length; i++)
			map.put(flds[i], vals[i]);
		this.keyEntryMap = map;
	}

	public String getTableName() {
		return tableName;
	}

	public Set<String> getKeyFldSet() {
		return keyEntryMap.keySet();
	}

	public Constant getKeyVal(String fld) {
		return keyEntryMap.get(fld);
	}

	public Predicate getPredicate() {
		Predicate pred = new Predicate();
		for (Entry<String, Constant> e : keyEntryMap.entrySet()) {
			Expression k = new FieldNameExpression(e.getKey());
			Expression v = new ConstantExpression(e.getValue());
			pred.conjunctWith(new Term(k, Term.OP_EQ, v));
		}
		return pred;
	}

	@Override
	public String toString() {
		return tableName + ":" + keyEntryMap.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != RecordKey.class)
			return false;
		RecordKey k = (RecordKey) obj;
		return k.tableName.equals(this.tableName)
				&& k.keyEntryMap.equals(this.keyEntryMap);
	}

	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + tableName.hashCode();
		hashCode = 31 * hashCode + keyEntryMap.hashCode();
		return hashCode;
	}

	/**
	 * Serialize this {@code CachedRecord} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		Set<String> fldsSet = keyEntryMap.keySet();
		out.defaultWriteObject();
		out.writeInt(fldsSet.size());

		// Write out all elements in the proper order
		for (String fld : fldsSet) {
			Constant val = keyEntryMap.get(fld);
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
		keyEntryMap = new HashMap<String, Constant>();
		int numFlds = in.readInt();

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			byte[] bytes = new byte[in.readInt()];
			in.read(bytes);
			Constant val = Constant.newInstance(Type.newInstance(sqlType),
					bytes);
			keyEntryMap.put(fld, val);
		}
	}
}
