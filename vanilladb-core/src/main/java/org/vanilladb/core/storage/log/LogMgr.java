package org.vanilladb.core.storage.log;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.tx.recovery.ReversibleIterator;
import org.vanilladb.core.util.CoreProperties;

/**
 * The low-level log manager. This log manager is responsible for writing log
 * records into a log file. A log record can be any sequence of integer and
 * string values. The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link org.vanilladb.core.storage.tx.recovery.RecoveryMgr recovery manager}.
 */
public class LogMgr implements Iterable<BasicLogRecord> {
	/**
	 * The location where the pointer to the last integer in the page is. A
	 * value of 0 means that the pointer is the first value in the page.
	 */
	public static final int LAST_POS = 0;
	public static final String LOG_FILE;

	// Optimization: store the size of pointer to other log record
	private int pointerSize = Page.maxSize(INTEGER);
	private Page myPage = new Page();
	private BlockId currentBlk;
	private int currentPos;

	private final Lock logMgrLock = new ReentrantLock();

	static {
		LOG_FILE = CoreProperties.getLoader().getPropertyAsString(LogMgr.class.getName()
				+ ".LOG_FILE", "vanilladb.log");
	}

	/**
	 * Creates the manager for the specified log file. If the log file does not
	 * yet exist, it is created with an empty first block. This constructor
	 * depends on a {@link FileMgr} object that it gets from the method
	 * {@link VanillaDb#fileMgr()}. That object is created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileMgr(String)} is called first.
	 * 
	 */
	public LogMgr() {
		long logsize = VanillaDb.fileMgr().size(LOG_FILE);
		if (logsize == 0)
			appendNewBlock();
		else {
			currentBlk = new BlockId(LOG_FILE, logsize - 1);
			myPage.read(currentBlk);
			currentPos = getLastRecordPosition() + pointerSize * 2;
		}
	}

	/**
	 * Ensures that the log records corresponding to the specified LSN has been
	 * written to disk. All earlier log records will also be written to disk.
	 * 
	 * @param lsn
	 *            the LSN of a log record
	 */
	public void flush(long lsn) {
		logMgrLock.lock();
		try {
			if (lsn >= currentLSN())
				flush();
		} finally {
			logMgrLock.unlock();
		}
	}

	/**
	 * Returns an iterator for the log records, which will be returned in
	 * reverse order starting with the most recent.
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public ReversibleIterator<BasicLogRecord> iterator() {
		logMgrLock.lock();
		try {
			flush();
			return new LogIterator(currentBlk);
		} finally {
			logMgrLock.unlock();
		}
	}

	/**
	 * Appends a log record to the file. The record contains an arbitrary array
	 * of values. The method also writes an integer to the end of each log
	 * record whose value is the offset of the corresponding integer for the
	 * previous log record. These integers allow log records to be read in
	 * reverse order.
	 * 
	 * @param rec
	 *            the list of values
	 * @return the LSN of the log record
	 */
	public long append(Constant[] rec) {
		logMgrLock.lock();
		try {
			
			// two integers that point to the previous and next log records
			int recsize = pointerSize * 2;
			for (Constant c : rec)
				recsize += Page.size(c);

			// if the log record doesn't fit, move to the next block
			if (currentPos + recsize >= BLOCK_SIZE) {
				flush();
				appendNewBlock();
			}
			for (Constant c : rec)
				appendVal(c);
			finalizeRecord();
			return currentLSN();
		} finally {
			logMgrLock.unlock();
		}
	}

	/**
	 * Remove the old log file and create a new one.
	 */
	public void removeAndCreateNewLog() {
		logMgrLock.lock();
		try {
			VanillaDb.fileMgr().rebuildLogFile();
			appendNewBlock();
		} finally {
			logMgrLock.unlock();
		}
	}

	/**
	 * Adds the specified value to the page at the position denoted by
	 * currentPos. Then increments currentPos by the size of the value.
	 * 
	 * @param val
	 *            the value to be added to the page
	 */
	private void appendVal(Constant val) {
		myPage.setVal(currentPos, val);
		currentPos += Page.size(val);
	}

	/**
	 * Returns the LSN of the most recent log record. As implemented, the LSN is
	 * the block number where the record is stored. Thus every log record in a
	 * block has the same LSN.
	 * 
	 * @return the LSN of the most recent log record
	 */
	private long currentLSN() {
		return currentBlk.number();
	}

	/**
	 * Writes the current page to the log file.
	 */
	private void flush() {
		myPage.write(currentBlk);
	}

	/**
	 * Clear the current page, and append it to the log file.
	 */
	private void appendNewBlock() {
		setLastRecordPosition(0);
		currentPos = pointerSize * 2;
		currentBlk = myPage.append(LOG_FILE);
	}

	/**
	 * Sets up a circular chain of pointers to the records in the page. There is
	 * an integer added to the end of each log record whose value is the offset
	 * of the previous log record. The first four bytes of the page contain an
	 * integer whose value is the offset of the integer for the last log record
	 * in the page.
	 */
	private void finalizeRecord() {
		myPage.setVal(currentPos, new IntegerConstant(getLastRecordPosition()));
		setPreviousNextRecordPosition(currentPos + pointerSize);
		setLastRecordPosition(currentPos);
		currentPos += pointerSize;
		setNextRecordPosition(currentPos);

		// leave for next pointer
		currentPos += pointerSize;
	}

	private int getLastRecordPosition() {
		return (Integer) myPage.getVal(LAST_POS, INTEGER).asJavaVal();
	}

	private void setLastRecordPosition(int pos) {
		myPage.setVal(LAST_POS, new IntegerConstant(pos));
	}

	private void setNextRecordPosition(int pos) {
		myPage.setVal(pos, new IntegerConstant(LAST_POS + pointerSize));
	}

	private void setPreviousNextRecordPosition(int pos) {
		int lastPos = (Integer) myPage.getVal(LAST_POS, INTEGER).asJavaVal();
		myPage.setVal(lastPos + pointerSize, new IntegerConstant(pos));
	}
}
