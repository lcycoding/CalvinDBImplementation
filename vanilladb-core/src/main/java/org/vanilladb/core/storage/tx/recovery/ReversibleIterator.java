package org.vanilladb.core.storage.tx.recovery;

import java.util.Iterator;

public interface ReversibleIterator<T> extends Iterator<T> {
	boolean hasNext();

	boolean hasPrevious();

	T next();

	T previous();

	void remove();
}
