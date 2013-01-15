package org.eulerdb.kernel;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.*;

import org.eulerdb.kernel.berkeleydb.EdbCursor;
import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.berkeleydb.EulerDBHelper;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbCaching;
import org.eulerdb.kernel.iterator.IteratorFactory;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class EdbGraph implements Graph {

	private static final Logger logger = Logger.getLogger(EdbGraph.class
			.getCanonicalName());

	public EdbKeyPairStore mNodePairs;
	public EdbKeyPairStore mEdgePairs;

	private EdbCaching mCache;

	public EdbGraph(String path) {
		EulerDBHelper.init(path);
		mCache = EdbCaching.getInstance();
		
		initStores(path);
	}

	@Override
	public Edge addEdge(Object weight, Vertex n1, Vertex n2, String relation) {
		
		EdbEdge e = new EdbEdge(n1, n2, weight, relation);
		store(mEdgePairs,e);
		((EdbVertex) n1).addOutEdge(e);
		((EdbVertex) n2).addInEdge(e);
		store(mNodePairs,n1);
		store(mNodePairs,n2);
		return e;
	}

	/**
	 * Object arg0: vertex
	 */
	@Override
	public Vertex addVertex(Object arg0) {
		store(mNodePairs, (EdbVertex)arg0);
		mCache.put((Integer) ((EdbVertex) arg0).getId(), (EdbVertex) arg0);
		return (EdbVertex)arg0;
	}

	@Override
	public Edge getEdge(Object arg0) {
		EdbEdge e = null;
		try {
			e = (EdbEdge) ByteArrayHelper.deserialize(mEdgePairs.get(ByteArrayHelper.serialize(arg0)));
		} catch (IOException e1) {
			e1.printStackTrace();
			logger.error(e);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			logger.error(e1);
		}
		return e;
	}

	@Override
	public Iterable<Edge> getEdges() {

		 return IteratorFactory.getEdgeIterator(mEdgePairs.getCursor());
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Features getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vertex getVertex(Object arg0) {
		EdbVertex n = mCache.get((Integer) arg0);
		if (n != null)
			return n;

		try {
			n = (EdbVertex) ByteArrayHelper.deserialize(mNodePairs
					.get(ByteArrayHelper.serialize((Integer) arg0)));
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			logger.error(e);
			e.printStackTrace();
		}

		mCache.put((Integer) arg0, n);

		return n;
	}

	@Override
	public Iterable<Vertex> getVertices() {
		
		 return IteratorFactory.getVertexIterator(mNodePairs.getCursor());
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeEdge(Edge arg0) {
		EdbEdge e2 = (EdbEdge) arg0;

		EdbVertex n = (EdbVertex) getVertex(e2.getVertex(Direction.IN));
		n.removeOutEdge(e2);
		store(mNodePairs, n);

		EdbVertex n2 = (EdbVertex) getVertex(e2.getVertex(Direction.OUT));
		n2.removeInEdge(e2);
		store(mNodePairs, n2);
		
		try {
			mEdgePairs.delete(ByteArrayHelper.serialize(e2.getId()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void removeVertex(Vertex arg0) {
		byte[] key = null;
		try {
			key = ByteArrayHelper.serialize((Integer) arg0.getId());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (Edge e : arg0.getEdges(Direction.IN, null)) {
			removeEdge(e);
		}
		
		for (Edge e : arg0.getEdges(Direction.OUT, null)) {
			removeEdge(e);
		}

		mCache.remove((Integer) arg0.getId());
		mNodePairs.delete(key);// remove(key);

	}

	@Override
	public void shutdown() {
		commit();
		mNodePairs.close();
		mEdgePairs.close();
		mNodePairs = null;
		mEdgePairs = null;

	}

	private void initStores(String path) {
		
		if (mNodePairs == null) {
			mNodePairs = new EdbKeyPairStore(Common.VERTEXSTORE);
		}
		
		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(Common.EDGESTORE);
		}

	}
	
	protected void store(EdbKeyPairStore store,Element n) {
		try {
			store.put(ByteArrayHelper.serialize(n.getId()),
					ByteArrayHelper.serialize(n));
			store.sync();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void commit() {
		mNodePairs.sync();
		mEdgePairs.sync();
	}

}