package org.vanilladb.core.storage.buffer;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;
import org.vanilladb.core.storage.tx.TransactionStartListener;
import org.vanilladb.core.util.CoreProperties;

/**
 * The publicly-accessible buffer manager. A buffer manager wraps a
 * {@link BasicBufferMgr} instance, and provides the same methods. The
 * difference is that the methods {@link #pin(BlockId)} and
 * {@link #pinNew(String, PageFormatter)} will never return false and null
 * respectively. If no buffers are currently available, then the calling thread
 * will be placed on a waiting list. The waiting threads are removed from the
 * list when a buffer becomes available. If a thread has been waiting for a
 * buffer for an excessive amount of time (currently, 10 seconds) then repins
 * all currently holding blocks by the calling transaction. Buffer manager
 * implements {@link TransactionStartListener} and
 * {@link TransactionLifecycleListener} for the purpose of unpinning buffers
 * when transaction commit/rollback/recovery.
 * 
 * <p>
 * A block must be pinned first before its getters/setters can be called.
 * </p>
 * 
 */
public class BufferMgrImpl implements BufferMgr {
	private static Logger logger = Logger.getLogger(BufferMgrImpl.class
			.getName());
	protected static final int BUFFER_POOL_SIZE;
	private static final long MAX_TIME;
	private static final long EPSILON;

	private BasicBufferMgr bufferMgr;
	// private Map<Long, List<Buffer>> pinnedByMap;
	// Optimization: Use hash map intends of linked list to record pinned buffer
	// for every tx
	private Map<Long, Map<BlockId, Buffer>> pinnedByMap;
	private List<Thread> waitingThreads;

	private final Object[] anchors = new Object[1009];

	static {
		MAX_TIME = CoreProperties.getLoader().getPropertyAsLong(
				BufferMgr.class.getName() + ".MAX_TIME", 10000);
		EPSILON = CoreProperties.getLoader().getPropertyAsLong(
				BufferMgr.class.getName() + ".EPSILON", 50);
		BUFFER_POOL_SIZE = CoreProperties.getLoader().getPropertyAsInteger(
				BufferMgr.class.getName() + ".BUFFER_POOL_SIZE", 1024);
	}

	/**
	 * Creates a new buffer manager having the specified number of buffers. This
	 * constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 */
	public BufferMgrImpl() {
		bufferMgr = new BasicBufferMgr(BUFFER_POOL_SIZE);
		pinnedByMap = new ConcurrentHashMap<Long, Map<BlockId, Buffer>>();
		waitingThreads = new LinkedList<Thread>();
		if (logger.isLoggable(Level.INFO))
			logger.info("buffer pool size " + BUFFER_POOL_SIZE);

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	@Override
	public void onTxCommit(Transaction tx) {
		unpinAll(tx);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		unpinAll(tx);
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	@Override
	public void onTxStart(Transaction tx) {
		tx.addLifecycleListener(this);
	}

	/**
	 * Pins a buffer to the specified block, potentially waiting until a buffer
	 * becomes available. If no buffer becomes available within a fixed time
	 * period, then repins all currently holding blocks.
	 * 
	 * @param blk
	 *            a block ID
	 * @param txNum
	 *            the calling transaction id
	 * @return the buffer pinned to that block
	 */
	public Buffer pin(BlockId blk, long txNum) {
		Map<BlockId, Buffer> bufferMap = pinnedByMap.get(txNum);
		/*
		 * Throws buffer abort exception if the calling tx requires buffers more
		 * than the size of buffer pool.
		 */
		if (bufferMap != null && bufferMap.size() == BUFFER_POOL_SIZE)
			throw new BufferAbortException();
		try {
			Buffer buff;
			long timestamp = System.currentTimeMillis();
			boolean waitedBeforeGotBuffer = false;
			while (true) {
				buff = bufferMgr.pin(blk);
				if (buff != null)
					break;
				
				waitedBeforeGotBuffer = true;
				synchronized (this) {
					if (!waitingThreads.contains(Thread.currentThread()))
						waitingThreads.add(Thread.currentThread());
					while (buff == null && !waitingTooLong(timestamp)) {
						wait(MAX_TIME);
						if (waitingThreads.get(0)
								.equals(Thread.currentThread()))
							break;
					}
					waitingThreads.remove(Thread.currentThread());
				}

			}

			if (buff == null) {
				repin(txNum);
				buff = pin(blk, txNum);
			} else {
				if (bufferMap == null) {
					bufferMap = new HashMap<BlockId, Buffer>();
					pinnedByMap.put(txNum, bufferMap);
				}
				bufferMap.put(buff.block(), buff);
			}
			if (waitedBeforeGotBuffer) {
				synchronized (this) {
					notifyAll();
				}
			}
			return buff;
		} catch (InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Pins a buffer to a new block in the specified file, potentially waiting
	 * until a buffer becomes available. If no buffer becomes available within a
	 * fixed time period, then repins all currently holding blocks.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            the formatter used to initialize the page
	 * @param txNum
	 *            the calling transaction id
	 * @return the buffer pinned to that block
	 */
	public Buffer pinNew(String fileName, PageFormatter fmtr, long txNum) {
		Map<BlockId, Buffer> bufferMap = pinnedByMap.get(txNum);
		/*
		 * throws buffer abort exception if the calling tx requires buffers more
		 * than the size of buffer pool
		 */

		if (bufferMap != null && bufferMap.size() == BUFFER_POOL_SIZE)
			throw new BufferAbortException();
		try {
			Buffer buff;
			long timestamp = System.currentTimeMillis();
			boolean waitedBeforeGotBuffer = false;
			while (true) {
				buff = bufferMgr.pinNew(fileName, fmtr);
				if (buff != null)
					break;
				waitedBeforeGotBuffer = true;
				synchronized (this) {
					if (!waitingThreads.contains(Thread.currentThread()))
						waitingThreads.add(Thread.currentThread());
					while (buff == null && !waitingTooLong(timestamp)) {
						wait(MAX_TIME);
						if (waitingThreads.get(0)
								.equals(Thread.currentThread()))
							break;
						// buff = bufferMgr.pin(blk);
					}
					waitingThreads.remove(Thread.currentThread());
				}

			}
			if (buff == null) {
				repin(txNum);
				buff = pinNew(fileName, fmtr, txNum);
			} else {
				bufferMap = pinnedByMap.get(txNum);
				if (bufferMap == null) {
					bufferMap = new HashMap<BlockId, Buffer>();
					pinnedByMap.put(txNum, bufferMap);
				}
				bufferMap.put(buff.block(), buff);
			}

			if (waitedBeforeGotBuffer) {
				synchronized (this) {
					notifyAll();
				}
			}
			return buff;
		} catch (InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Unpins the specified buffer. If the buffer's pin count becomes 0, then
	 * the threads on the wait list are notified.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	public void unpin(long txNum, Buffer... buffs) {
		boolean isFullBeforeUnpin = (bufferMgr.available() < 1);
		boolean existAnyUnpinned = false;

		Map<BlockId, Buffer> bufferMap = pinnedByMap.get(txNum);

		for (Buffer buff : buffs) {
			BlockId blk = buff.block();
			bufferMgr.unpin(buff);
			// Optimization: Using hash map to reduce searching time
			if (bufferMap != null)
				bufferMap.remove(blk);
			if (!buff.isPinned()) {
				existAnyUnpinned = true;
			}
		}

		if (isFullBeforeUnpin && existAnyUnpinned) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	/**
	 * Flushes all dirty buffers.
	 */
	public void flushAll() {
		bufferMgr.flushAll();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	public void flushAll(long txNum) {
		bufferMgr.flushAll(txNum);
	}

	/**
	 * Returns the number of available (ie unpinned) buffers.
	 * 
	 * @return the number of available buffers`
	 */
	public int available() {
		return bufferMgr.available();
	}

	private void unpinAll(Transaction tx) {
		long txNum = tx.getTransactionNumber();

		Map<BlockId, Buffer> bufferMap = pinnedByMap.get(txNum);
		// Tx may not pin any buffer
		if (bufferMap != null) {
			Collection<Buffer> buffs = bufferMap.values();

			if (buffs != null)
				unpin(txNum, buffs.toArray(new Buffer[0]));
			pinnedByMap.remove(txNum);
		}
	}

	/**
	 * Unpins all currently pinned buffers of the calling transaction and repins
	 * them.
	 */
	private void repin(long txNum) {
		try {
			Map<BlockId, Buffer> currentPinnedBuffs = pinnedByMap.get(txNum);
			if (currentPinnedBuffs == null)
				return;
			Collection<Buffer> buffs = new LinkedList<Buffer>(
					currentPinnedBuffs.values());
			unpin(txNum, buffs.toArray(new Buffer[0]));
			synchronized (this) {
				wait(MAX_TIME);
			}
			for (Buffer buff : buffs)
				pin(buff.block(), txNum);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private boolean waitingTooLong(long startTime) {
		return System.currentTimeMillis() - startTime + EPSILON > MAX_TIME;
	}
}
