package org.eulerdb.kernel;


import java.util.Iterator;
import org.eulerdb.kernel.berkeleydb.EdbCursor;
import com.tinkerpop.blueprints.Vertex;


public class EdbVertexIterator implements Iterable<Vertex>, Iterator<Vertex> {
	
	EdbCursor mCur;
	
	public EdbVertexIterator(EdbCursor cur) {
		mCur = cur;
	}

	@Override
	public boolean hasNext() {
		
		return mCur.hasNext();
	}

	@Override
	public Vertex next() {
		
		return mCur.next();
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
