package org.vanilladb.core.storage.file.io.javanio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

public class JavaNioFileChannel implements IoChannel {

	private FileChannel fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public JavaNioFileChannel(File file) throws FileNotFoundException {
		@SuppressWarnings("resource")
		RandomAccessFile f = new RandomAccessFile(file, "rws");
		fileChannel = f.getChannel();
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		JavaNioByteBuffer javaBuffer = (JavaNioByteBuffer) buffer;
		return fileChannel.read(javaBuffer.getByteBuffer(), position);
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		JavaNioByteBuffer javaBuffer = (JavaNioByteBuffer) buffer;
		return fileChannel.write(javaBuffer.getByteBuffer(), position);
	}

	@Override
	public long size() throws IOException {
		return fileChannel.size();
	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
	}

	@Override
	public ReentrantReadWriteLock getReadWriteLock() {
		return lock;
	}
}
