package org.eulerdb.kernel.iterator;


import java.util.Iterator;

import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;

import com.tinkerpop.blueprints.Vertex;


public class EdbVertexIteratorFromDatabase extends EdbVertexIterator {
	
	EdbCursor mCur;
	
	public EdbVertexIteratorFromDatabase(EdbCursor cur) {
		mCur = cur;
	}

	@Override
	public boolean hasNext() {
		
		boolean result =  mCur.hasNext();
		if(!result) {
			mCur.close();
		}
		return result;
	}

	@Override
	public Vertex next() {
		
		return (Vertex) mCur.next();
	}

	@Override
	public void remove() {
		mCur.remove();
		
	}

	@Override
	public Iterator<Vertex> iterator() {
		
		return this;
	}
	
	

}
