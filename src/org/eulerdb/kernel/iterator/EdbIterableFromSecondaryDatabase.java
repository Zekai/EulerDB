package org.eulerdb.kernel.iterator;

import java.util.Iterator;

import org.eulerdb.kernel.storage.EdbSecondaryCursor;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

public class EdbIterableFromSecondaryDatabase<T>  implements Iterable<T> {
	
	EdbSecondaryCursor mCur;
	EdbStorage mStorage;
	
	public EdbIterableFromSecondaryDatabase(final EdbSecondaryCursor cur) {
		mCur = cur;
		mStorage = EdbStorage.getInstance();
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
				//FIXME: 
				return (T) mCur.next();
			}

			@Override
			public void remove() {
				mCur.remove();
				
			}
			 
		 };
	}
	
	

}
