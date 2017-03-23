package org.vanilladb.core.storage.file.io;

import java.io.File;
import java.io.IOException;

import org.vanilladb.core.storage.file.io.javanio.JavaNioByteBuffer;
import org.vanilladb.core.storage.file.io.javanio.JavaNioFileChannel;
import org.vanilladb.core.storage.file.io.jaydio.JaydioDirectByteBuffer;
import org.vanilladb.core.storage.file.io.jaydio.JaydioDirectIoChannel;
import org.vanilladb.core.util.CoreProperties;

public class IoAllocator {

	private static boolean USE_O_DIRECT;

	static {
		USE_O_DIRECT = CoreProperties.getLoader().getPropertyAsBoolean(
				IoAllocator.class.getName() + ".USE_O_DIRECT", false);
	}

	public static IoBuffer newIoBuffer(int capacity) {
		if (USE_O_DIRECT)
			return new JaydioDirectByteBuffer(capacity);
		else
			return new JavaNioByteBuffer(capacity);
	}

	public static IoChannel newIoChannel(File file) throws IOException {
		if (USE_O_DIRECT)
			return new JaydioDirectIoChannel(file);
		else
			return new JavaNioFileChannel(file);
	}
}
