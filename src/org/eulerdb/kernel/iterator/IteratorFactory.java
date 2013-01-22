package org.eulerdb.kernel.iterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;

public class IteratorFactory {
	public static EdbVertexIterator getVertexIterator(Object arg0) {

		if (arg0 instanceof EdbCursor) {
			return (new EdbVertexIteratorFromDatabase((EdbCursor)arg0));
		} else {
			return (new EdbVertexIteratorFromCollection((Collection<EdbVertex>) arg0));
		}
	}
	
	
	public static EdbEdgeIterator getEdgeIterator(Object arg0) {

		if (arg0 instanceof EdbCursor) {
			return (new EdbEdgeIteratorFromDatabase((EdbCursor)arg0));
		} else {
			return (new EdbEdgeIteratorFromCollection((Collection<EdbEdge>) arg0));
		}
	}
}
