package org.eulerdb.kernel.iterator;


import java.util.Iterator;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdgeIterableFromIterator extends EdbEdgeIterable {
	
	private Iterator<EdbEdge> mIt;

	public EdbEdgeIterableFromIterator(Iterator<EdbEdge> it) {
		mIt = it;
	}

	@Override
	public Iterator<Edge> iterator() {
		final Iterator<EdbEdge> it = mIt;
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
