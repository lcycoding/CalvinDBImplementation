package org.vanilladb.core.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TxComponentTimer {

	private static class SubTimer {
		private long start = 0, startTimes = 0, totalTime = 0, count = 0;

		public void startTimer() {
			if (startTimes == 0)
				start = System.nanoTime();

			startTimes++;
			count++;
		}

		public void stopTimer() {
			startTimes--;

			if (startTimes == 0)
				totalTime += (System.nanoTime() - start) / 1000;
		}

		public long getTotalTime() {
			return totalTime;
		}

		public long getCount() {
			return count;
		}
	}

	private Map<Object, SubTimer> subTimers = new HashMap<Object, SubTimer>();
	private List<Object> componenents = new LinkedList<Object>();
	private Object[] metadata;
	private long executionStart, executionTime = 0;

	private long txNum;

	public TxComponentTimer(long txNum) {
		this.txNum = txNum;
	}

	public void startComponentTimer(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer == null) {
			timer = new SubTimer();
			subTimers.put(component, timer);
			componenents.add(component);
		}
		timer.startTimer();
	}

	public void stopComponentTimer(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer != null)
			timer.stopTimer();
	}

	public long getComponentCount(Object component) {
		return subTimers.get(component).getCount();
	}

	public void startExecution() {
		executionStart = System.nanoTime();
	}

	public void stopExecution() {
		executionTime = (System.nanoTime() - executionStart) / 1000;
	}

	public long getComponentTime(Object component) {
		SubTimer timer = subTimers.get(component);
		if (timer == null)
			return -1;
		return timer.getTotalTime();
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public long getTransactionNumber() {
		return txNum;
	}

	public Object getMetadata() {
		return metadata;
	}

	public void setMetadata(Object[] m) {
		this.metadata = m;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		if (!componenents.isEmpty()) {
			sb.append("===== Profile for Tx." + txNum + " =====\n");
			for (Object com : componenents)
				sb.append(String.format("%-40s: %d us, with %d counts\n", com,
						subTimers.get(com).getTotalTime(), subTimers.get(com)
								.getCount()));
			sb.append(String.format("%-40s: %d us\n", "Execution Time:",
					executionTime));
			sb.append("==============================\n");
		}

		return sb.toString();
	}
}
