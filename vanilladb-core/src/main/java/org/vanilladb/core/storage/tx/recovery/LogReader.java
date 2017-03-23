package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_CHECKPOINT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_COMMIT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_DELETE;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_INDEX_INSERT;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_ROLLBACK;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.OP_START;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.log.BasicLogRecord;

public class LogReader {
	private static final int LAST_POS_POINTER = 0;
	
	// Page
	private int pointerSize = Page.maxSize(Type.INTEGER);
	private Page page = new Page();
	private BlockId currentBlk;
	private int currentPos;
	
	// Log File
	private long fileSize; // number of blocks
	
	// Log Record
	private LogRecord currentRec;
	
	public LogReader(String logFileName) {
		fileSize = VanillaDb.fileMgr().size(logFileName);
		currentBlk = new BlockId(logFileName, 0); 
		currentPos = pointerSize * 2; // point to first record
		page.read(currentBlk);
	}

	public boolean nextRecord() {
		// check if there is record at current position
		if (getLastRecordPosition() == currentPos - pointerSize * 2) {
			// check if there is the next block
			if (hasNextBlock()) {
				moveToNextBlock();
				
				if (hasRecordInCurrentBlock())
					return nextRecord();
				else
					return false;
			} else
				return false;
		}
		
		// get record
		currentRec = readRecord(new BasicLogRecord(page, currentPos));
			
		// move to next record position
		int nextPos = (Integer) page.getVal(currentPos - pointerSize, Type.INTEGER).asJavaVal();
		currentPos = nextPos + pointerSize;
		
		return true;
	}
	
	public String getLogString() {
		return currentRec.toString();
	}
	
	private LogRecord readRecord(BasicLogRecord rec) {
		int op = (Integer) rec.nextVal(INTEGER).asJavaVal();
		switch (op) {
		case OP_CHECKPOINT:
			return new CheckpointRecord(rec);
		case OP_START:
			return new StartRecord(rec);
		case OP_COMMIT:
			return new CommitRecord(rec);
		case OP_ROLLBACK:
			return new RollbackRecord(rec);
		case OP_INDEX_DELETE:
			return new IndexDeleteRecord(rec);
		case OP_INDEX_INSERT:
			return new IndexInsertRecord(rec);
		default:
			return new SetValueRecord(rec, op);
		}
	}
	
	private void moveToNextBlock() {
		BlockId nextBlk = new BlockId(currentBlk.fileName(), currentBlk.number() + 1);
		page.read(nextBlk);
		currentBlk = nextBlk;
		currentPos = pointerSize * 2; // point to first record
	}
	
	private boolean hasNextBlock() {
		if (currentBlk.number() < fileSize - 1)
			return true;
		return false;
	}
	
	private boolean hasRecordInCurrentBlock() {
		return getLastRecordPosition() != 0;
	}
	
	private int getLastRecordPosition() {
		return (Integer) page.getVal(LAST_POS_POINTER, Type.INTEGER).asJavaVal();
	}
}
