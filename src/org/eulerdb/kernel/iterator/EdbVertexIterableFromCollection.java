package org.eulerdb.kernel.iterator;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIterableFromCollection extends EdbVertexIterable {
	
	private Collection<Vertex> mIt;

	public EdbVertexIterableFromCollection(Collection<Vertex> it) {
		mIt = it;
	}

	@Override
	public Iterator<Vertex> iterator() {
		final Iterator<Vertex> it = mIt.iterator();
		
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