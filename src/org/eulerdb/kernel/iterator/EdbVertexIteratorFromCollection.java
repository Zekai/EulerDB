package org.eulerdb.kernel.iterator;

import java.io.IOException;
import java.util.Iterator;

import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIteratorFromCollection extends EdbVertexIterator {
	
	private Iterator<EdbVertex> mIt;
	private Integer mCurrent;

	public EdbVertexIteratorFromCollection(Iterator<EdbVertex> it) {
		mIt = it;
		mCurrent = 0;
	}

	@Override
	public boolean hasNext() {
		return mIt.hasNext();
	}

	@Override
	public Vertex next() {
		
		return mIt.next();
	}

	@Override
	public void remove() {
		mIt.remove();
		
	}

	@Override
	public Iterator<Vertex> iterator() {
		
		return this;
	}
}
