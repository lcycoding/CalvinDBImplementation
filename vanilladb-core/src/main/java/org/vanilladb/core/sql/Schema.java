package org.vanilladb.core.sql;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The schema of a table, which contains the name and type of each field of the
 * table.
 */
public class Schema implements Serializable {

	private static final long serialVersionUID = 253681L;

	private transient Map<String, Type> fields = new HashMap<String, Type>();

	private SortedSet<String> myFieldSet;

	/**
	 * Creates an empty schema.
	 */
	public Schema() {
	}

	/**
	 * Adds a field to this schema having a specified name and type.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param type
	 *            the type of the field, according to the constants in
	 *            {@link Type}
	 */
	public void addField(String fldName, Type type) {
		fields.put(fldName, type);
		if (myFieldSet != null)
			myFieldSet.add(fldName);
	}

	/**
	 * Adds a field in another schema having the specified name to this schema.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param sch
	 *            the other schema
	 */
	public void add(String fldName, Schema sch) {
		Type type = sch.type(fldName);
		addField(fldName, type);
	}

	/**
	 * Adds all of the fields in the specified schema to this schema.
	 * 
	 * @param sch
	 *            the other schema
	 */
	public void addAll(Schema sch) {
		fields.putAll(sch.fields);
		if (myFieldSet != null)
			myFieldSet = new TreeSet<String>(fields.keySet());
	}

	/**
	 * Returns a sorted set containing the field names in this schema, sorted by
	 * their natural ordering.
	 * 
	 * @return the sorted set of the schema's field names
	 */
	public SortedSet<String> fields() {
		// Optimization: Materialize the fields set
		if (myFieldSet == null)
			myFieldSet = new TreeSet<String>(fields.keySet());
		return myFieldSet;
	}

	/**
	 * Returns true if the specified field is in this schema.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return true if the field is in this schema
	 */
	public boolean hasField(String fldName) {
		return fields().contains(fldName);
	}

	/**
	 * Returns the type of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the type of the field
	 */
	public Type type(String fldName) {
		return fields.get(fldName);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		Set<String> flds = fields();
		for (String fld : flds)
			sb.append(fld).append(" ").append(fields.get(fld)).append(", ");
		int end = sb.length();
		sb.replace(end - 2, end, "] ");
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(Schema.class)))
			return false;
		Schema s = (Schema) obj;
		return toString().equals(s.toString());
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/**
	 * Serialize this {@code Schema} instance.
	 * 
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		Set<String> fldsSet = fields.keySet();
		out.defaultWriteObject();
		out.writeInt(fldsSet.size());

		// Write out all elements in the proper order
		for (String fld : fldsSet) {
			out.writeObject(fld);
			out.writeInt(fields.get(fld).getSqlType());
			out.writeInt(fields.get(fld).getArgument());
		}
	}

	private void readObject(ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		fields = new HashMap<String, Type>();
		int numFlds = in.readInt();

		// Read in all elements and rebuild the map
		for (int i = 0; i < numFlds; i++) {
			String fld = (String) in.readObject();
			int sqlType = in.readInt();
			int arg = in.readInt();
			fields.put(fld, Type.newInstance(sqlType, arg));
		}
	}

}
