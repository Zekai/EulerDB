package org.eulerdb.kernel.iterator;


import java.util.Iterator;
import org.eulerdb.kernel.storage.EdbCursor;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;


public class EdbEdgeIterableFromDatabase extends EdbEdgeIterable {
	
	EdbCursor mCur;
	
	public EdbEdgeIterableFromDatabase(EdbCursor cur) {
		mCur = cur;
	}

	@Override
	public Iterator<Edge> iterator() {
		
		mCur.getFirst();
		
		 return new Iterator<Edge>() {

			@Override
			public boolean hasNext() {
				return mCur.hasNext();
			}

			@Override
			public Edge next() {
				return (Edge) mCur.next();
			}

			@Override
			public void remove() {
				mCur.remove();
				
			}
			 
		 };
	}
	
	

}
