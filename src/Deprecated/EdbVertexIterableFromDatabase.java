package Deprecated;


import java.util.Iterator;
import org.eulerdb.kernel.storage.EdbPrimaryCursor;

import com.tinkerpop.blueprints.Vertex;


public class EdbVertexIterableFromDatabase extends EdbVertexIterable {
	
	EdbPrimaryCursor mCur;
	
	public EdbVertexIterableFromDatabase(EdbPrimaryCursor cur) {
		mCur = cur;
	}

	@Override
	public Iterator<Vertex> iterator() {
		mCur.getFirst();
		
		 return new Iterator<Vertex>() {

			@Override
			public boolean hasNext() {
				return mCur.hasNext();
			}

			@Override
			public Vertex next() {
				return (Vertex) mCur.next();
			}

			@Override
			public void remove() {
				mCur.remove();
				
			}
			 
		 };
	}
	
	

}
