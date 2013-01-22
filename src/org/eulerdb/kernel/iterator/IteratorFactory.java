package org.eulerdb.kernel.iterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;

public class IteratorFactory {
	public static EdbVertexIterable getVertexIterator(Object arg0) {

		if (arg0 instanceof EdbCursor) {
			return (new EdbVertexIterableFromDatabase((EdbCursor)arg0));
		} else if (arg0 instanceof Collection){
			return (new EdbVertexIterableFromCollection((Collection<EdbVertex>) arg0));
		} else {
			return (new EdbVertexIterableFromIterator((Iterator<EdbVertex>) arg0));
		}
	}
	
	
	public static EdbEdgeIterable getEdgeIterator(Object arg0) {

		if (arg0 instanceof EdbCursor) {
			return (new EdbEdgeIterableFromDatabase((EdbCursor)arg0));
		} else if (arg0 instanceof Collection){
			return (new EdbEdgeIterableFromCollection((Collection<EdbEdge>) arg0));
		} else {
			return (new EdbEdgeIterableFromIterator((Iterator<EdbEdge>) arg0));
		}
	}
}
