package org.eulerdb.kernel.iterator;


import java.util.Iterator;
import org.eulerdb.kernel.EdbVertex;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIteratorFromCollection extends EdbVertexIterator {
	
	private Iterator<EdbVertex> mIt;

	public EdbVertexIteratorFromCollection(Iterator<EdbVertex> it) {
		mIt = it;
	}

	@Override
	public boolean hasNext() {
		return mIt.hasNext();
	}

	@Override
	public Vertex next() {
		
		return mIt.next();
	}

	@Override
	public void remove() {
		mIt.remove();
		
	}

	@Override
	public Iterator<Vertex> iterator() {
		
		return this;
	}
}
