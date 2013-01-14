package org.eulerdb.kernel;

import java.util.Iterator;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

public class EdbEdgeIterator implements Iterable<Edge>, Iterator<Edge> {

	private Iterator<Integer> mIterator;

	public EdbEdgeIterator(Iterator<Integer> it) {
		mIterator = it;
	}

	@Override
	public boolean hasNext() {

		return mIterator.hasNext();
	}

	@Override
	public Edge next() {

		return null;
	}

	@Override
	public void remove() {

	}

	@Override
	public Iterator<Edge> iterator() {

		return this;
	}

}
