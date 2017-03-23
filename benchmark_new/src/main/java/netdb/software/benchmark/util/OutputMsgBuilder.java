package netdb.software.benchmark.util;

public class OutputMsgBuilder {
	private StringBuilder sb;

	public OutputMsgBuilder() {
		sb = new StringBuilder();
	}

	public OutputMsgBuilder append(Object... os) {
		for (Object o : os)
			append(o.toString());
		return this;
	}

	public OutputMsgBuilder append(int i) {
		sb.append(i).append(" ");
		return this;
	}

	public OutputMsgBuilder append(double d) {
		sb.append(d).append(" ");
		return this;
	}

	public OutputMsgBuilder append(long l) {
		sb.append(l).append(" ");
		return this;
	}

	public OutputMsgBuilder append(String s) {
		sb.append(s).append(" ");
		return this;
	}

	public String build() {
		return sb.toString();
	}
}