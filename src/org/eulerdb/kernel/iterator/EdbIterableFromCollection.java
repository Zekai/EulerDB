package org.eulerdb.kernel.iterator;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;
import com.tinkerpop.blueprints.Vertex;

public class EdbIterableFromCollection<T> implements Iterable<T> {
	
	private Collection<T> mIt;

	public EdbIterableFromCollection(Collection<T> it) {
		mIt = it;
	}

	@Override
	public Iterator<T> iterator() {
		final Iterator<T> it = mIt.iterator();
		
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