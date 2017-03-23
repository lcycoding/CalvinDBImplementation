package netdb.software.benchmark.util;

import java.math.BigDecimal;

public class DoublePlainPrinter {
	public static String toPlainString(double d) {
		return (new BigDecimal(Double.toString(d))).toPlainString();
	}
}
