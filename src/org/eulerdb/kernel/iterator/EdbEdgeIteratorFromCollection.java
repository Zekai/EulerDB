package org.eulerdb.kernel.iterator;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdgeIteratorFromCollection extends EdbEdgeIterator {
	
	private Collection<EdbEdge> mIt;

	public EdbEdgeIteratorFromCollection(Collection<EdbEdge> it) {
		mIt = it;
	}

	@Override
	public Iterator<Edge> iterator() {
		final Iterator<EdbEdge> it = mIt.iterator();
		
		 return new Iterator<Edge>() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public Edge next() {
				return it.next();
			}

			@Override
			public void remove() {
				it.remove();
				
			}
			 
		 };
	}
}
