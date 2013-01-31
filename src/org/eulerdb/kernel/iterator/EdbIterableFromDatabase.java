package org.eulerdb.kernel.iterator;


import java.util.Iterator;
import org.eulerdb.kernel.storage.EdbCursor;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class EdbIterableFromDatabase<T> implements Iterable<T> {
	
	EdbCursor mCur;
	
	public EdbIterableFromDatabase(EdbCursor cur) {
		mCur = cur;
	}

	@Override
	public Iterator<T> iterator() {
		
		mCur.getFirst();
		
		 return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return mCur.hasNext();
			}

			@Override
			public T next() {
				return (T) mCur.next();
			}

			@Override
			public void remove() {
				mCur.remove();
				
			}
			 
		 };
	}
	
	

}
