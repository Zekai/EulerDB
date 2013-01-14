package org.eulerdb.kernel;

import java.io.IOException;
import java.util.Iterator;

import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.tinkerpop.blueprints.Vertex;

public class EdbIVertexIterator implements Iterable<Vertex>, Iterator<Vertex>{
	
	private Iterator<Integer> mIt;
	private EdbKeyPairStore mStore;
	private Integer mCurrent;

	public EdbIVertexIterator(EdbKeyPairStore store,Iterator<Integer> it) {
		mStore = store;
		mIt = it;
		mCurrent = 0;
	}

	@Override
	public boolean hasNext() {
		return mIt.hasNext();
	}

	@Override
	public Vertex next() {
		EdbVertex v = null;
		mCurrent =  mIt.next();
		try {
			v = (EdbVertex) ByteArrayHelper.deserialize(mStore.get(ByteArrayHelper.serialize(mCurrent)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return v;
	}

	@Override
	public void remove() {
		try {
			mStore.delete(ByteArrayHelper.serialize(mCurrent));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public Iterator<Vertex> iterator() {
		
		return this;
	}
}
