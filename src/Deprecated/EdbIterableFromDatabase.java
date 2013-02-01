package Deprecated;


import java.util.Iterator;
import org.eulerdb.kernel.storage.EdbPrimaryCursor;


public class EdbIterableFromDatabase<T> implements Iterable<T> {
	
	EdbPrimaryCursor mCur;
	
	public EdbIterableFromDatabase(EdbPrimaryCursor cur) {
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
