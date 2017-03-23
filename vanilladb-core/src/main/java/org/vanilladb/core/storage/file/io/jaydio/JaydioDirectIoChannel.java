package org.vanilladb.core.storage.file.io.jaydio;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

public class JaydioDirectIoChannel implements IoChannel {

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
	}
	
	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
		return fileChannel.read(jaydioBuffer.getAlignedDirectByteBuffer(), position);
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
		return fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), position);
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
