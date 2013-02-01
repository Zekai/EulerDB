package org.eulerdb.kernel.iterator;

import java.util.Iterator;

import org.eulerdb.kernel.storage.EdbBaseCursor;
import org.eulerdb.kernel.storage.EdbSecondaryCursor;
import org.eulerdb.kernel.storage.EdbStorage;

import com.tinkerpop.blueprints.Element;

public class EdbIterableFromDatabase<T, S extends EdbBaseCursor>  implements Iterable<T> {
	
	private S mCur;
	
	public EdbIterableFromDatabase(S cur) {
		mCur = cur;
	}

	//@Override
	public Iterator<T> iterator() {
		mCur.getFirst();
		
		 return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return  mCur.hasNext();
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
