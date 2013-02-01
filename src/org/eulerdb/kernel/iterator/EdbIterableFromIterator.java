package org.eulerdb.kernel.iterator;


import java.util.Iterator;

public class EdbIterableFromIterator<T> implements Iterable<T> {
	
	private Iterator<T> mIt;

	public EdbIterableFromIterator(Iterator<T> it) {
		mIt = it;
	}

	@Override
	public Iterator<T> iterator() {
		final Iterator<T> it = mIt;
		 return new Iterator<T>() {

				@Override
				public boolean hasNext() {
					return it.hasNext();
				}

				@Override
				public T next() {
					return (T) it.next();
				}

				@Override
				public void remove() {
					it.remove();
					
				}
				 
			 };
	}
}
