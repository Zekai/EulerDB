package org.eulerdb.kernel.iterator;

import java.util.Iterator;
import java.util.Map.Entry;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.storage.EdbKeyPairStore;

import com.google.common.collect.Multimap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

public class EdbEdgeIteratorFromCollection extends EdbEdgeIterator {

	private Iterator<EdbEdge> mEdgesIterator;

	public EdbEdgeIteratorFromCollection(Iterator<EdbEdge> iterator) {
		mEdgesIterator = iterator;
	}

	@Override
	public boolean hasNext() {

		return mEdgesIterator.hasNext();
	}

	@Override
	public Edge next() {
		return mEdgesIterator.next();
	}

	@Override
	public void remove() {
		mEdgesIterator.remove();
	}

	@Override
	public Iterator<Edge> iterator() {

		return this;
	}

}
