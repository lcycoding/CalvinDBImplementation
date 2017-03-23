package org.vanilladb.core.storage.buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BasicDummyBufferMgr {
	private Map<BlockId, Buffer> blockMap;
	private final Object[] anchors = new Object[1009];

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs
	 *            the number of buffer slots to allocate
	 */
	BasicDummyBufferMgr() {
		blockMap = new ConcurrentHashMap<BlockId, Buffer>();
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	private Object prepareAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0) {
			code += anchors.length;
		}
		return anchors[code];
	}

	/**
	 * Flushes all dirty buffers.
	 */
	// XXX
	void flushAll() {
		for (Buffer buff : blockMap.values()) {
			synchronized (prepareAnchor(buff.block())) {
				buff.flush();
			}
		}
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	void flushAll(long txNum) {
		for (Buffer buff : blockMap.values()) {
			synchronized (prepareAnchor(buff.block())) {
				if (buff.isModifiedBy(txNum))
					buff.flush();
			}
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a block ID
	 * @return the pinned buffer
	 */
	Buffer pin(BlockId blk) {
		Buffer buff;
		synchronized (prepareAnchor(blk)) {
			buff = findExistingBuffer(blk);
			if (buff == null) {
				buff = new Buffer();
				buff.assignToBlock(blk);
				blockMap.put(blk, buff);
			}
			buff.pin();
		}
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	Buffer pinNew(String fileName, PageFormatter fmtr) {
		synchronized (prepareAnchor(fileName)) {
			Buffer buff = new Buffer();
			buff.assignToNew(fileName, fmtr);
			buff.pin();
			blockMap.put(buff.block(), buff);
			return buff;
		}
	}

	int available() {
		return 1;
	}

	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		for (Buffer buff : buffs) {
			buff.unpin();
			if (!buff.isPinned())
				synchronized (prepareAnchor(buff.block())) {
					if (!buff.shouldBeBuffered() && !buff.isPinned()) {
						blockMap.remove(buff.block());
						buff.flush();
						buff.close();
					}
				}
		}
	}

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = blockMap.get(blk);
		if (buff != null && buff.block().equals(blk))
			return buff;
		return null;
	}

}
