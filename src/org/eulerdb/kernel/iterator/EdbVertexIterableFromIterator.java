package org.eulerdb.kernel.iterator;


import java.util.Iterator;
import org.eulerdb.kernel.EdbVertex;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIterableFromIterator extends EdbVertexIterable {
	
	private Iterator<EdbVertex> mIt;

	public EdbVertexIterableFromIterator(Iterator<EdbVertex> it) {
		mIt = it;
	}

	@Override
	public Iterator<Vertex> iterator() {
		final Iterator<EdbVertex> it = mIt;
		 return new Iterator<Vertex>() {

				@Override
				public boolean hasNext() {
					return it.hasNext();
				}

				@Override
				public Vertex next() {
					return it.next();
				}

				@Override
				public void remove() {
					it.remove();
					
				}
				 
			 };
	}
}
