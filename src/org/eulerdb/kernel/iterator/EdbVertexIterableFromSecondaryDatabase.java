package org.eulerdb.kernel.iterator;

import java.util.Iterator;

import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;
import org.eulerdb.kernel.storage.EdbSecondaryCursor;

import com.sleepycat.je.SecondaryCursor;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIterableFromSecondaryDatabase extends EdbVertexIterable {
	
	EdbSecondaryCursor mCur;
	
	public EdbVertexIterableFromSecondaryDatabase(final EdbSecondaryCursor cur) {
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
				//FIXME: this is not a good idea, it works only because my current EdbVertex only store id
				return new EdbVertex(mCur.next());
			}

			@Override
			public void remove() {
				mCur.remove();
				
			}
			 
		 };
	}
	
	

}
