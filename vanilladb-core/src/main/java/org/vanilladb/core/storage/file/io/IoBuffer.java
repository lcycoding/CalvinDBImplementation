package org.vanilladb.core.storage.file.io;

public interface IoBuffer {

	IoBuffer get(int position, byte[] dst);

	IoBuffer put(int position, byte[] src);

	void clear();

	void rewind();

	void close();

}
