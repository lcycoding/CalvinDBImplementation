package org.vanilladb.core.storage.tx.concurrency;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.CoreProperties;

/**
 * Checks the compatibility of locking requests on a single item (e.g., file,
 * block, or record). Does <em>not</em> implement a locking protocol to ensure
 * the semantic correctness of locking different items with different
 * granularity.
 * 
 * <p>
 * If a transaction requests to lock an item that causes a conflict with an
 * existing lock on that item, then the transaction is placed into a wait list
 * and will be notified (awaked) to compete for the lock whenever the conflict
 * is resolved. Currently, there is only one wait list for all items.
 * </p>
 */
class LockTable {
	private static final long MAX_TIME;
	private static final long EPSILON;
	final static int IS_LOCK = 0, IX_LOCK = 1, S_LOCK = 2, SIX_LOCK = 3,
			X_LOCK = 4;

	static {
		MAX_TIME = CoreProperties.getLoader().getPropertyAsLong(
				LockTable.class.getName() + ".MAX_TIME", 10000);
		EPSILON = CoreProperties.getLoader().getPropertyAsLong(LockTable.class.getName()
				+ ".EPSILON", 50);
	}

	class Lockers {
		List<Long> sLockers, ixLockers, isLockers;
		// only one tx can hold xLock(sixLock) on single item
		long sixLocker, xLocker;
		static final long NONE = -1; // for sixLocker, xLocker
		Set<Long> requestSet;

		Lockers() {
			sLockers = new LinkedList<Long>();
			ixLockers = new LinkedList<Long>();
			isLockers = new LinkedList<Long>();
			sixLocker = NONE;
			xLocker = NONE;
			requestSet = new HashSet<Long>();
		}
	}

	class LocktableNotifier extends Task {

		@Override
		public void run() {
			while (true) {
				try {
					Long txNum = toBeNotified.take();
					Object anchor = txWaitMap.get(txNum);

					if (anchor != null) {
						synchronized (anchor) {
							anchor.notifyAll();
						}

					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private Map<Object, Lockers> lockerMap = new HashMap<Object, Lockers>();
	private Map<Long, Set<Object>> lockByMap = new ConcurrentHashMap<Long, Set<Object>>();
	private Set<Long> txnsToBeAborted = Collections
			.synchronizedSet(new HashSet<Long>());
	private Map<Long, Object> txWaitMap = new ConcurrentHashMap<Long, Object>();
	private BlockingQueue<Long> toBeNotified = new ArrayBlockingQueue<Long>(
			1000);
	private final Object anchors[] = new Object[1009];

	public LockTable() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
		VanillaDb.taskMgr().runTask(new LocktableNotifier());
	}

	private Object getAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0) {
			code += anchors.length;
		}
		return anchors[code];
	}

	private void avoidDeadlock(Lockers lks, long txNum, int lockType)
			throws LockAbortException {
		// IS_LOCK = 0, IX_LOCK = 1, S_LOCK = 2, SIX_LOCK = 3, X_LOCK = 4

		if (txnsToBeAborted.contains(txNum))
			throw new LockAbortException();

		if (lockType == IX_LOCK || lockType == SIX_LOCK || lockType == X_LOCK) {
			for (Long tx : lks.sLockers) {
				if (tx > txNum) {
					txnsToBeAborted.add(tx);
					if (!toBeNotified.contains(tx))
						toBeNotified.add(tx);
				}
			}
		}
		if (lockType == S_LOCK || lockType == SIX_LOCK || lockType == X_LOCK) {
			for (Long tx : lks.ixLockers) {
				if (tx > txNum) {
					txnsToBeAborted.add(tx);
					if (!toBeNotified.contains(tx))
						toBeNotified.add(tx);
				}
			}
		}
		if (lockType == X_LOCK) {
			for (Long tx : lks.isLockers) {
				if (tx > txNum) {
					txnsToBeAborted.add(tx);
					if (!toBeNotified.contains(tx))
						toBeNotified.add(tx);
				}
			}
		}
		if (lockType == IX_LOCK || lockType == S_LOCK || lockType == SIX_LOCK
				|| lockType == X_LOCK) {
			if (lks.sixLocker > txNum) {
				txnsToBeAborted.add(lks.sixLocker);
				if (!toBeNotified.contains(lks.sixLocker))
					toBeNotified.add(lks.sixLocker);
			}
		}
		if (lks.xLocker > txNum) {
			txnsToBeAborted.add(lks.xLocker);
			if (!toBeNotified.contains(lks.xLocker))
				toBeNotified.add(lks.xLocker);
		}
	}

	/**
	 * Grants an slock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	void sLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);
		txWaitMap.put(txNum, anchor);
		synchronized (anchor) {
			Lockers lks = prepareLockers(obj);

			if (hasSLock(lks, txNum))
				return;

			try {
				long timestamp = System.currentTimeMillis();
				while (!sLockable(lks, txNum) && !waitingTooLong(timestamp)) {
					avoidDeadlock(lks, txNum, S_LOCK);
					lks.requestSet.add(txNum);

					anchor.wait(MAX_TIME);
					lks.requestSet.remove(txNum);
				}
				if (!sLockable(lks, txNum))
					throw new LockAbortException();
				lks.sLockers.add(txNum);
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException();
			}
		}
		txWaitMap.remove(txNum);
	}

	/**
	 * Grants an xlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	void xLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);
		txWaitMap.put(txNum, anchor);
		synchronized (anchor) {
			Lockers lks = prepareLockers(obj);

			if (hasXLock(lks, txNum))
				return;

			try {
				long timestamp = System.currentTimeMillis();
				while (!xLockable(lks, txNum) && !waitingTooLong(timestamp)) {
					avoidDeadlock(lks, txNum, X_LOCK);
					lks.requestSet.add(txNum);

					anchor.wait(MAX_TIME);
					lks.requestSet.remove(txNum);
				}
				if (!xLockable(lks, txNum))
					throw new LockAbortException();
				lks.xLocker = txNum;
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException();
			}
		}
		txWaitMap.remove(txNum);
	}

	/**
	 * Grants an sixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	void sixLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);
		txWaitMap.put(txNum, anchor);
		synchronized (anchor) {
			Lockers lks = prepareLockers(obj);

			if (hasSixLock(lks, txNum))
				return;

			try {
				long timestamp = System.currentTimeMillis();
				while (!sixLockable(lks, txNum) && !waitingTooLong(timestamp)) {
					avoidDeadlock(lks, txNum, SIX_LOCK);
					lks.requestSet.add(txNum);

					anchor.wait(MAX_TIME);
					lks.requestSet.remove(txNum);
				}
				if (!sixLockable(lks, txNum))
					throw new LockAbortException();
				lks.sixLocker = txNum;
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException();
			}
		}
		txWaitMap.remove(txNum);
	}

	/**
	 * Grants an islock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 */
	void isLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);
		txWaitMap.put(txNum, anchor);
		synchronized (anchor) {
			Lockers lks = prepareLockers(obj);
			if (hasIsLock(lks, txNum))
				return;
			try {
				long timestamp = System.currentTimeMillis();
				while (!isLockable(lks, txNum) && !waitingTooLong(timestamp)) {
					avoidDeadlock(lks, txNum, IS_LOCK);
					lks.requestSet.add(txNum);

					anchor.wait(MAX_TIME);
					lks.requestSet.remove(txNum);
				}
				if (!isLockable(lks, txNum))
					throw new LockAbortException();
				lks.isLockers.add(txNum);
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException();
			}
		}
		txWaitMap.remove(txNum);
	}

	/**
	 * Grants an ixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 */
	void ixLock(Object obj, long txNum) {
		Object anchor = getAnchor(obj);
		txWaitMap.put(txNum, anchor);
		synchronized (anchor) {
			Lockers lks = prepareLockers(obj);

			if (hasIxLock(lks, txNum))
				return;

			try {
				long timestamp = System.currentTimeMillis();
				while (!ixLockable(lks, txNum) && !waitingTooLong(timestamp)) {
					avoidDeadlock(lks, txNum, IX_LOCK);
					lks.requestSet.add(txNum);

					anchor.wait(MAX_TIME);
					lks.requestSet.remove(txNum);
				}
				if (!ixLockable(lks, txNum))
					throw new LockAbortException();
				lks.ixLockers.add(txNum);
				getObjectSet(txNum).add(obj);
			} catch (InterruptedException e) {
				throw new LockAbortException();
			}
		}
		txWaitMap.remove(txNum);
	}

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	void release(Object obj, long txNum, int lockType) {
		Object anchor = getAnchor(obj);
		synchronized (anchor) {
			Lockers lks = lockerMap.get(obj);
			/*
			 * In some situation, tx will release the lock of the object that
			 * have been released.
			 */
			if (lks != null) {
				releaseLock(lks, anchor, txNum, lockType);

				// Check if this transaction have any other lock on this object
				if (!hasSLock(lks, txNum) && !hasXLock(lks, txNum)
						&& !hasSixLock(lks, txNum) && !hasIsLock(lks, txNum)
						&& !hasIxLock(lks, txNum)) {
					getObjectSet(txNum).remove(obj);

					// Remove the locker, if there is no other transaction
					// having it
					if (!sLocked(lks) && !xLocked(lks) && !sixLocked(lks)
							&& !isLocked(lks) && !ixLocked(lks)
							&& lks.requestSet.isEmpty())
						lockerMap.remove(obj);
				}
			}
		}
	}

	/**
	 * Releases all locks held by a transaction. If a lock is the last lock on
	 * that block, then the waiting transactions are notified.
	 * 
	 * @param txNum
	 *            a transaction number
	 * 
	 * @param sLockOnly
	 *            release slocks only
	 */
	void releaseAll(long txNum, boolean sLockOnly) {
		Set<Object> objectsToRelease = getObjectSet(txNum);
		for (Object obj : objectsToRelease) {
			Object anchor = getAnchor(obj);
			synchronized (anchor) {
				Lockers lks = lockerMap.get(obj);

				if (lks != null) {

					if (hasSLock(lks, txNum))
						releaseLock(lks, anchor, txNum, S_LOCK);

					if (hasXLock(lks, txNum) && !sLockOnly)
						releaseLock(lks, anchor, txNum, X_LOCK);

					if (hasSixLock(lks, txNum))
						releaseLock(lks, anchor, txNum, SIX_LOCK);

					while (hasIsLock(lks, txNum))
						releaseLock(lks, anchor, txNum, IS_LOCK);

					while (hasIxLock(lks, txNum) && !sLockOnly)
						releaseLock(lks, anchor, txNum, IX_LOCK);

					// Remove the locker, if there is no other transaction
					// having it
					if (!sLocked(lks) && !xLocked(lks) && !sixLocked(lks)
							&& !isLocked(lks) && !ixLocked(lks)
							&& lks.requestSet.isEmpty())
						lockerMap.remove(obj);
				}
			}
		}
		txWaitMap.remove(txNum);
		txnsToBeAborted.remove(txNum);
		lockByMap.remove(txNum);
	}

	private void releaseLock(Lockers lks, Object anchor, long txNum,
			int lockType) {
		if (lks == null)
			return;
		anchor.notifyAll();
		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;

				anchor.notifyAll();
			}
			return;
		case SIX_LOCK:
			if (lks.sixLocker == txNum) {
				lks.sixLocker = -1;

				anchor.notifyAll();
			}
			return;
		case S_LOCK:
			List<Long> sl = lks.sLockers;
			if (sl != null && sl.contains(txNum)) {
				sl.remove((Long) txNum);
				if (sl.isEmpty()) {

					anchor.notifyAll();
				}
			}
			return;
		case IS_LOCK:
			List<Long> isl = lks.isLockers;
			if (isl != null && isl.contains(txNum)) {
				isl.remove((Long) txNum);
				if (isl.isEmpty()) {

					anchor.notifyAll();
				}
			}
			return;
		case IX_LOCK:
			List<Long> ixl = lks.ixLockers;
			if (ixl != null && ixl.contains(txNum)) {
				ixl.remove((Long) txNum);
				if (ixl.isEmpty()) {

					anchor.notifyAll();
				}
			}
			return;
		default:
			throw new IllegalArgumentException();
		}
	}

	private Lockers prepareLockers(Object obj) {
		Lockers lockers = lockerMap.get(obj);
		if (lockers == null) {
			lockers = new Lockers();
			lockerMap.put(obj, lockers);
		}
		return lockers;
	}

	private Set<Object> getObjectSet(long txNum) {
		Set<Object> objectSet = lockByMap.get(txNum);
		if (objectSet == null) {
			objectSet = new HashSet<Object>();
			lockByMap.put(txNum, objectSet);
		}
		return objectSet;
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime + EPSILON > MAX_TIME;
	}

	/*
	 * Verify if an item is locked.
	 */

	private boolean sLocked(Lockers lks) {
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Lockers lks) {
		return lks != null && lks.xLocker != -1;
	}

	private boolean sixLocked(Lockers lks) {
		return lks != null && lks.sixLocker != -1;
	}

	private boolean isLocked(Lockers lks) {
		return lks != null && lks.isLockers.size() > 0;
	}

	private boolean ixLocked(Lockers lks) {
		return lks != null && lks.ixLockers.size() > 0;
	}

	/*
	 * Verify if an item is held by a tx.
	 */

	private boolean hasSLock(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.contains(txNum);
	}

	private boolean hasXLock(Lockers lks, long txNUm) {
		return lks != null && lks.xLocker == txNUm;
	}

	private boolean hasSixLock(Lockers lks, long txNum) {
		return lks != null && lks.sixLocker == txNum;
	}

	private boolean hasIsLock(Lockers lks, long txNum) {
		return lks != null && lks.isLockers.contains(txNum);
	}

	private boolean hasIxLock(Lockers lks, long txNum) {
		return lks != null && lks.ixLockers.contains(txNum);
	}

	private boolean isTheOnlySLocker(Lockers lks, long txNum) {
		return lks != null && lks.sLockers.size() == 1
				&& lks.sLockers.contains(txNum);
	}

	private boolean isTheOnlyIsLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.isLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	private boolean isTheOnlyIxLocker(Lockers lks, long txNum) {
		if (lks != null) {
			for (Object o : lks.ixLockers)
				if (!o.equals(txNum))
					return false;
			return true;
		}
		return false;
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean sLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum));
	}

	private boolean xLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!isLocked(lks) || isTheOnlyIsLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean sixLockable(Lockers lks, long txNum) {
		return (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!ixLocked(lks) || isTheOnlyIxLocker(lks, txNum))
				&& (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean ixLockable(Lockers lks, long txNum) {
		return (!sLocked(lks) || isTheOnlySLocker(lks, txNum))
				&& (!sixLocked(lks) || hasSixLock(lks, txNum))
				&& (!xLocked(lks) || hasXLock(lks, txNum));
	}

	private boolean isLockable(Lockers lks, long txNum) {
		return (!xLocked(lks) || hasXLock(lks, txNum));
	}
}
