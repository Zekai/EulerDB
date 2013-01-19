package org.eulerdb.kernel.iterator;

import java.util.Iterator;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;

import com.sleepycat.je.Cursor;

public class IteratorFactory {
	public static EdbVertexIterator getVertexIterator(Object arg0) {

		if (arg0 instanceof Cursor) {
			return (new EdbVertexIteratorFromDatabase(new EdbCursor ((Cursor)arg0)));
		} else {
			return (new EdbVertexIteratorFromCollection((Iterator<EdbVertex>) arg0));
		}
	}
	
	
	public static EdbEdgeIterator getEdgeIterator(Object arg0) {

		if (arg0 instanceof Cursor) {
			return (new EdbEdgeIteratorFromDatabase(new EdbCursor ((Cursor)arg0)));
		} else {
			return (new EdbEdgeIteratorFromCollection((Iterator<EdbEdge>) arg0));
		}
	}
}
