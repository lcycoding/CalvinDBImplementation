package org.vanilladb.core.storage.buffer;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;
import org.vanilladb.core.storage.tx.TransactionStartListener;

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
public interface BufferMgr extends TransactionStartListener,
		TransactionLifecycleListener {

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
	public Buffer pin(BlockId blk, long txNum);

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
	public Buffer pinNew(String fileName, PageFormatter fmtr, long txNum);

	/**
	 * Unpins the specified buffer. If the buffer's pin count becomes 0, then
	 * the threads on the wait list are notified.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	public void unpin(long txNum, Buffer... buffs);

	/**
	 * Flushes all dirty buffers.
	 */
	public void flushAll();

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	public void flushAll(long txNum);

	/**
	 * Returns the number of available (ie unpinned) buffers.
	 * 
	 * @return the number of available buffers`
	 */
	public int available();
}
