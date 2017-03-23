package org.vanilladb.core.storage.file;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.log.LogMgr.LOG_FILE;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.io.IoAllocator;
import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;
import org.vanilladb.core.util.CoreProperties;

/**
 * The VanillaDb file manager. The database system stores its data as files
 * within a specified directory. The file manager provides methods for reading
 * the contents of a file block to a Java byte buffer, writing the contents of a
 * byte buffer to a file block, and appending the contents of a byte buffer to
 * the end of a file. These methods are called exclusively by the class
 * {@link org.vanilladb.core.storage.file.Page Page}, and are thus
 * package-private. The class also contains two public methods: Method
 * {@link #isNew() isNew} is called during system initialization by
 * {@link VanillaDb#init}. Method {@link #size(String) size} is called by the
 * log manager and transaction manager to determine the end of the file.
 */

public class FileMgr {

	public static final String HOME_DIR, LOG_FILE_BASE_DIR;
	public static final String TMP_FILE_NAME_PREFIX = "_temp";

	private static Logger logger = Logger.getLogger(FileMgr.class.getName());
	private File dbDirectory, logDirectory;
	private boolean isNew;
	private Map<String, IoChannel> openFiles = new ConcurrentHashMap<String, IoChannel>();

	// Optimization: store the size of each table
	private Map<String, Long> fileSizeMap = new ConcurrentHashMap<String, Long>();

	static {
		HOME_DIR = CoreProperties.getLoader().getPropertyAsString(
				FileMgr.class.getName() + ".HOME_DIR",
				System.getProperty("user.home"));
		LOG_FILE_BASE_DIR = CoreProperties.getLoader().getPropertyAsString(
				FileMgr.class.getName() + ".LOG_FILE_BASE_DIR", HOME_DIR);
	}

	private final Object[] anchors = new Object[1009];

	private Object prepareAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0)
			code += anchors.length;
		return anchors[code];
	}

	/**
	 * Creates a file manager for the specified database. The database will be
	 * stored in a folder of that name in the user's home directory. If the
	 * folder does not exist, then a folder containing an empty database is
	 * created automatically. Files for all temporary tables (i.e. tables
	 * beginning with "_temp") will be deleted during initializing.
	 * 
	 * @param dbName
	 *            the name of the directory that holds the database
	 */
	public FileMgr(String dbName) {
		dbDirectory = new File(HOME_DIR, dbName);

		// the log file can be specified to be stored in different location
		logDirectory = new File(LOG_FILE_BASE_DIR, dbName);
		isNew = !dbDirectory.exists();

		// deal with the log folder in new database
		if (isNew && !dbDirectory.equals(logDirectory)) {
			// delete the old log file if db is new
			if (logDirectory.exists()) {
				deleteLogFiles();
			} else if (!logDirectory.mkdir())
				throw new RuntimeException("cannot create log file for"
						+ dbName);
		}

		// check the existence of log folder
		if (!isNew && !logDirectory.exists())
			throw new RuntimeException("log file for the existed " + dbName
					+ " is missing");

		// create the directory if the database is new
		if (isNew && (!dbDirectory.mkdir()))
			throw new RuntimeException("cannot create " + dbName);

		// remove any leftover temporary tables
		for (String filename : dbDirectory.list())
			if (filename.startsWith(TMP_FILE_NAME_PREFIX))
				new File(dbDirectory, filename).delete();

		if (logger.isLoggable(Level.INFO))
			logger.info("block size " + Page.BLOCK_SIZE);

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	/**
	 * Reads the contents of a disk block into a byte buffer.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the byte buffer
	 */
	void read(BlockId blk, IoBuffer bb) {
		try {
			IoChannel fileChannel = getFileChannel(blk.fileName());
			
			// clear the buffer
			bb.clear();
			
			// read a block from file
			fileChannel.getReadWriteLock().readLock().lock();
			try {
				fileChannel.read(bb, blk.number() * BLOCK_SIZE);
			} finally {
				fileChannel.getReadWriteLock().readLock().unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	/**
	 * Writes the contents of a byte buffer into a disk block.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the byte buffer
	 */
	void write(BlockId blk, IoBuffer bb) {
		try {
			IoChannel fileChannel = getFileChannel(blk.fileName());
			fileChannel.getReadWriteLock().writeLock().lock();
			
			// rewind the buffer
			bb.rewind();
			
			// write a block to file
			try {
				fileChannel.write(bb, blk.number() * BLOCK_SIZE);
				// Optimization:
				if (blk.number() + 1 > fileSizeMap.get(blk.fileName()))
					fileSizeMap.put(blk.fileName(), blk.number() + 1);
			} finally {
				fileChannel.getReadWriteLock().writeLock().unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	/**
	 * Appends the contents of a byte buffer to the end of the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param bb
	 *            the byte buffer
	 * @return a block ID refers to the newly-created block.
	 */
	BlockId append(String fileName, IoBuffer bb) {
		// Optimization: Only returning reference to a new block
		IoChannel fileChannel;
		try {
			fileChannel = getFileChannel(fileName);
			fileChannel.getReadWriteLock().writeLock().lock();
			try {
				// get the next block number from file
				// Old method:
				// newblknum = fileChannel.size() / BLOCK_SIZE;
				// Optimization:
				long newblknum = fileSizeMap.get(fileName);
				BlockId blk = new BlockId(fileName, newblknum);
				
				// rewind the buffer for writing
				bb.rewind();
				
				// write a block to a new position of the file
				// FIXME: We can use another method to avoid writing this
				// this optimization can cause throughput increases by 10%
				fileChannel.write(bb, blk.number() * BLOCK_SIZE);
				fileSizeMap.put(fileName, ++newblknum);
				return blk;
			} finally {
				fileChannel.getReadWriteLock().writeLock().unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the number of blocks in the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * 
	 * @return the number of blocks in the file
	 */
	public long size(String fileName) {
		try {
			IoChannel fileChannel = getFileChannel(fileName);
			
			// Old method:
			// synchronized (fc) {
			// return fc.size() / BLOCK_SIZE;
			// }

			// Optimization:
			fileChannel.getReadWriteLock().readLock().lock();
			try {
				return fileSizeMap.get(fileName);
			} finally {
				fileChannel.getReadWriteLock().readLock().unlock();
			}

		} catch (IOException e) {
			throw new RuntimeException("cannot access " + fileName);
		}
	}

	/**
	 * Returns a boolean indicating whether the file manager had to create a new
	 * database directory.
	 * 
	 * @return true if the database is new
	 */
	public boolean isNew() {
		return isNew;
	}

	/**
	 * Deletes all old log files and builds new log files.
	 */
	public void rebuildLogFile() {
		try {
			deleteLogFiles();

			// Create a new log file
			File logFile = new File(logDirectory, LOG_FILE);
			IoChannel fileChannel = IoAllocator.newIoChannel(logFile);
			openFiles.put(LOG_FILE, fileChannel);
			fileSizeMap.put(LOG_FILE, fileChannel.size() / BLOCK_SIZE);

			// Create a new DD log file
			logFile = new File(logDirectory, "vanilladddb.log");
			fileChannel = IoAllocator.newIoChannel(logFile);
			openFiles.put("vanilladddb.log", fileChannel);
			fileSizeMap.put("vanilladddb.log", fileChannel.size() / BLOCK_SIZE);

		} catch (IOException e) {
			throw new RuntimeException("rebuild log file fail");
		}
	}

	/**
	 * Returns the file channel for the specified filename. The file channel is
	 * stored in a map keyed on the filename. If the file is not open, then it
	 * is opened and the file channel is added to the map.
	 * 
	 * @param fileName
	 *            the specified filename
	 * 
	 * @return the file channel associated with the open file.
	 * @throws IOException
	 */
	private IoChannel getFileChannel(String fileName) throws IOException {
		synchronized (prepareAnchor(fileName)) {
			IoChannel fileChannel = openFiles.get(fileName);

			if (fileChannel == null) {
				File dbFile = fileName.equals(LOG_FILE) ? new File(
						logDirectory, fileName) : new File(dbDirectory,
						fileName);
				fileChannel = IoAllocator.newIoChannel(dbFile);

				openFiles.put(fileName, fileChannel);

				// Optimization:
				fileSizeMap.put(fileName, fileChannel.size() / BLOCK_SIZE);
			}
			
			return fileChannel;
		}
	}

	/**
	 * Deletes all log files in the log directory.
	 */
	private void deleteLogFiles() {
		try {
			for (String fileName : logDirectory.list())
				if (fileName.endsWith(".log")) {
					synchronized (prepareAnchor(fileName)) {
						// Close file, if it opened
						IoChannel fileChannel = openFiles.remove(fileName);
						if (fileChannel != null)
							fileChannel.close();

						// Actually delete file
						boolean hasDeleted = new File(logDirectory, fileName)
								.delete();
						if (!hasDeleted && logger.isLoggable(Level.WARNING))
							logger.warning("cannot deleted old log file");
					}
				}
		} catch (IOException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("there is something wrong when deleting log files");
			e.printStackTrace();
		}
	}
}