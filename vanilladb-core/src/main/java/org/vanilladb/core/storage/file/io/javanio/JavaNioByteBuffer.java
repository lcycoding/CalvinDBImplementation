package org.vanilladb.core.storage.file.io.javanio;

import java.nio.ByteBuffer;

import org.vanilladb.core.storage.file.io.IoBuffer;

public class JavaNioByteBuffer implements IoBuffer {

	private ByteBuffer byteBuffer;
	
	public JavaNioByteBuffer(int capacity) {
		byteBuffer = ByteBuffer.allocateDirect(capacity);
	}
	
	@Override
	public IoBuffer get(int position, byte[] dst) {
		byteBuffer.position(position);
		byteBuffer.get(dst);
		return this;
	}

	@Override
	public IoBuffer put(int position, byte[] src) {
		byteBuffer.position(position);
		byteBuffer.put(src);
		return this;
	}

	ByteBuffer getByteBuffer() {
		return byteBuffer;
	}
	
	@Override
	public void clear() {
		byteBuffer.clear();
	}

	@Override
	public void rewind() {
		byteBuffer.rewind();
	}

	@Override
	public void close() {
		// do nothing
	}
}
