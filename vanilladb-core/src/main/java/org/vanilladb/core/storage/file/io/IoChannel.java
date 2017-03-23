package org.vanilladb.core.storage.file.io;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface IoChannel {
	
	int read(IoBuffer buffer, long position) throws IOException;
	
	int write(IoBuffer buffer, long position) throws IOException;
	
	long size() throws IOException;
	
	void close() throws IOException;
	
	ReentrantReadWriteLock getReadWriteLock();
}
