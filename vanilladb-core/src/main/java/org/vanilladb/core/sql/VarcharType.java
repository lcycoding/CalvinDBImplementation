package org.vanilladb.core.sql;

import java.nio.charset.Charset;

import org.vanilladb.core.util.CoreProperties;

public class VarcharType extends Type {
	/**
	 * The name of charset used to encode/decode strings.
	 */
	public static final String CHAR_SET;
	// Optimization: store the number of bytes per char
	private static int bytesPerChar;
	/**
	 * Argument. -1 means undefined.
	 */
	private int argument = -1;

	static {
		CHAR_SET = CoreProperties.getLoader().getPropertyAsString(
				VarcharType.class.getName() + ".CHAR_SET", "UTF-8");
		bytesPerChar = (int) Charset.forName(CHAR_SET).newEncoder()
				.maxBytesPerChar();
	}

	VarcharType() {
	}

	VarcharType(int arg) {
		this.argument = arg;
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.VARCHAR;
	}

	@Override
	public int getArgument() {
		return this.argument;
	}

	@Override
	public boolean isFixedSize() {
		return false;
	}

	@Override
	public boolean isNumeric() {
		return false;
	}

	/**
	 * Returns the maximum number of bytes required, by following the rule
	 * specified in {@link VarcharConstant#getBytes}, to encode a
	 * {@link Constant value} of this type.
	 */
	@Override
	public int maxSize() {
		// unlimited capacity if argument is not specified
		if (this.argument == -1)
			return Integer.MAX_VALUE;
		return this.argument * bytesPerChar;
	}

	@Override
	public Constant maxValue() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Empty string is the minimal value by following the rules in
	 * {@link String#compareTo}.
	 */
	@Override
	public Constant minValue() {
		return new VarcharConstant("");
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof VarcharType))
			return false;
		VarcharType t = (VarcharType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "VARCHAR(" + this.argument + ")";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

}
