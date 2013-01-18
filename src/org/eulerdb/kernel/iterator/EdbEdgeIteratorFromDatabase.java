package org.eulerdb.kernel.iterator;

import java.util.Iterator;

import org.eulerdb.kernel.berkeleydb.EdbCursor;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdgeIteratorFromDatabase extends EdbEdgeIterator {

	EdbCursor mCur;

	public EdbEdgeIteratorFromDatabase(EdbCursor cur) {
		mCur = cur;
	}

	@Override
	public boolean hasNext() {

		boolean result =  mCur.hasNext();
		if(!result) {
			mCur.close(); 
		}
		return result;
	}

	@Override
	public Edge next() {

		return (Edge) mCur.next();
	}

	@Override
	public void remove() {
		mCur.remove();

	}
	

	@Override
	public Iterator<Edge> iterator() {

		return this;
	}
	
	@Override
	public void finalize() {
		if(mCur!=null) mCur.close();
	}

}
