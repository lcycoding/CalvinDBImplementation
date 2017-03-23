package org.vanilladb.core.storage.buffer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;

/**
 * An individual buffer. A buffer wraps a page and stores information about its
 * status, such as the disk block associated with the page, the number of times
 * the block has been pinned, whether the contents of the page have been
 * modified, and if so, the id of the modifying transaction and the LSN of the
 * corresponding log record.
 */
public class Buffer {
	private Page contents = new Page();
	private BlockId blk = null;
	private int pins = 0;
	private boolean isNew = false;
	private Set<Long> modifiedBy = new HashSet<Long>();
	// negative means no corresponding log record
	private long maxLsn = -1;
	private boolean shouldBeBuffered;

	private final Lock externalLock = new ReentrantLock();
	private final ReentrantReadWriteLock internalLock = new ReentrantReadWriteLock();

	/**
	 * Creates a new buffer, wrapping a new {@link Page page}. This constructor
	 * is called exclusively by the class {@link BasicBufferMgr}. It depends on
	 * the {@link org.vanilladb.core.storage.log.LogMgr LogMgr} object that it
	 * gets from the class {@link VanillaDb}. That object is created during
	 * system initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 */
	Buffer() {
	}

	/**
	 * Returns the value at the specified offset of this buffer's page. If an
	 * integer was not stored at that location, the behavior of the method is
	 * unpredictable.
	 * 
	 * @param offset
	 *            the byte offset of the page
	 * @param type
	 *            the type of the value
	 * 
	 * @return the constant value at that offset
	 */
	public Constant getVal(int offset, Type type) {
		internalLock.readLock().lock();
		try {
			return contents.getVal(offset, type);
		} finally {
			internalLock.readLock().unlock();
		}
	}

	/**
	 * Writes a value to the specified offset of this buffer's page. This method
	 * assumes that the transaction has already written an appropriate log
	 * record. The buffer saves the id of the transaction and the LSN of the log
	 * record. A negative lsn value indicates that a log record was not
	 * necessary.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * @param val
	 *            the new value to be written
	 * @param txNum
	 *            the id of the transaction performing the modification
	 * @param lsn
	 *            the LSN of the corresponding log record
	 */
	public void setVal(int offset, Constant val, long txNum, long lsn) {
		internalLock.writeLock().lock();
		try {
			modifiedBy.add(txNum);
			if (lsn >= 0)
				maxLsn = lsn;
			contents.setVal(offset, val);
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Returns a block ID refers to the disk block that the buffer is pinned to.
	 * 
	 * @return a block ID
	 */
	public BlockId block() {
		internalLock.readLock().lock();
		try {
			return blk;
		} finally {
			internalLock.readLock().unlock();
		}
	}

	/**
	 * Writes the page to its disk block if the page is dirty. The method
	 * ensures that the corresponding log record has been written to disk prior
	 * to writing the page to disk.
	 */
	void flush() {
		internalLock.writeLock().lock();
		try {
			if (isNew || modifiedBy.size() > 0) {
				VanillaDb.logMgr().flush(maxLsn);
				contents.write(blk);
				modifiedBy.clear();
				isNew = false;
			}
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Increases the buffer's pin count.
	 */
	void pin() {
		internalLock.writeLock().lock();
		try {
			pins++;
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Decreases the buffer's pin count.
	 */
	void unpin() {
		internalLock.writeLock().lock();
		try {
			pins--;
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Returns true if the buffer is currently pinned (that is, if it has a
	 * nonzero pin count).
	 * 
	 * @return true if the buffer is pinned
	 */
	boolean isPinned() {
		internalLock.readLock().lock();
		try {
			return pins > 0;
		} finally {
			internalLock.readLock().unlock();
		}
	}

	/**
	 * Returns true if the buffer is dirty due to a modification by the
	 * specified transaction.
	 * 
	 * @param txNum
	 *            the id of the transaction
	 * @return true if the transaction modified the buffer
	 */
	boolean isModifiedBy(long txNum) {
		internalLock.writeLock().lock();
		try {
			return modifiedBy.contains(txNum);
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Reads the contents of the specified block into the buffer's page. If the
	 * buffer was dirty, then the contents of the previous page are first
	 * written to disk.
	 * 
	 * @param blk
	 *            a block ID
	 */
	void assignToBlock(BlockId blk) {
		internalLock.writeLock().lock();
		try {
			flush();
			this.blk = blk;
			// if (block().fileName().startsWith("idx_"))
			contents.read(blk);
			pins = 0;
			shouldBeBuffered = (blk.fileName().startsWith("tblcat")
					|| blk.fileName().startsWith("idxcat")
					|| blk.fileName().startsWith("fldcat") || (blk.fileName()
					.startsWith("idx_") && blk.fileName().endsWith("dir.tbl")));
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	/**
	 * Initializes the buffer's page according to the specified formatter, and
	 * appends the page to the specified file. If the buffer was dirty, then the
	 * contents of the previous page are first written to disk.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a page formatter, used to initialize the page
	 */
	void assignToNew(String fileName, PageFormatter fmtr) {
		internalLock.writeLock().lock();
		try {
			flush();
			fmtr.format(contents);
			blk = contents.append(fileName);
			pins = 0;
			isNew = true;
			shouldBeBuffered = (blk.fileName().startsWith("tblcat")
					|| blk.fileName().startsWith("idxcat")
					|| blk.fileName().startsWith("fldcat") || (blk.fileName()
					.startsWith("idx_") && blk.fileName().endsWith("dir.tbl")));
		} finally {
			internalLock.writeLock().unlock();
		}
	}

	boolean shouldBeBuffered() {
		internalLock.readLock().lock();
		try {
			return shouldBeBuffered;
		} finally {
			internalLock.readLock().unlock();
		}
	}

	protected Lock getExternalLock() {
		return externalLock;
	}

	protected void close() {
		internalLock.writeLock().lock();
		try {
			contents.close();
		} finally {
			internalLock.writeLock().unlock();
		}
	}
}