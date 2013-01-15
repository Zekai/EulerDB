package org.eulerdb.kernel.iterator;


import java.util.Iterator;
import org.eulerdb.kernel.berkeleydb.EdbCursor;
import com.tinkerpop.blueprints.Vertex;


public class EdbVertexIteratorFromDatabase extends EdbVertexIterator {
	
	EdbCursor mCur;
	
	public EdbVertexIteratorFromDatabase(EdbCursor cur) {
		mCur = cur;
	}

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

	@Override
	public Iterator<Vertex> iterator() {
		
		return this;
	}
	
	

}
