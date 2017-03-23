package org.vanilladb.core.util;

public class Timers {

	private static final int THREAD_BOUND = 2000;

	private static TxComponentTimer[] timers = new TxComponentTimer[THREAD_BOUND];

	private static TxComponentTimer defaultTimer = new TxComponentTimer(-1);

	public static void createTimer(long txNum, Object... metadata) {
		int threadId = (int) Thread.currentThread().getId();
		timers[threadId] = new TxComponentTimer(txNum);
		timers[threadId].setMetadata(metadata);
	}

	public static TxComponentTimer getTimer() {
		int threadId = (int) Thread.currentThread().getId();

		if (timers[threadId] == null)
			return defaultTimer;

		return timers[threadId];
	}

	public static void reportTime() {
		int threadId = (int) Thread.currentThread().getId();

		String profile = timers[threadId].toString();

		if (profile != null && !profile.isEmpty())
			System.out.println(profile);

		timers[threadId] = null;
	}
}
