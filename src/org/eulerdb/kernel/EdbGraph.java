package org.eulerdb.kernel;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.*;

import org.eulerdb.kernel.berkeleydb.EdbCursor;
import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.berkeleydb.EulerDBHelper;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbCaching;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class EdbGraph implements Graph {

	private static final Logger logger = Logger.getLogger(EdbGraph.class
			.getCanonicalName());

	public EdbKeyPairStore edgeStore;

	private EdbCaching mCache;

	public EdbGraph(String path) {

		mCache = EdbCaching.getInstance();
		edgeStore = EdbKeyPairStore.getInstance(path);
		
		initStores(path);

	}

	@Override
	public Edge addEdge(Object weight, Vertex n1, Vertex n2, String relation) {

		EdbEdge e = new EdbEdge(n1, n2, weight, relation);
		((EdbVertex) n1).addOutEdge(e);
		((EdbVertex) n2).addInEdge(e);
		store(n1);
		store(n2);
		return null;
	}

	/**
	 * Object arg0: vertex
	 */
	@Override
	public Vertex addVertex(Object arg0) {
		store((EdbVertex) arg0);
		mCache.put((Integer) ((EdbVertex) arg0).getId(), (EdbVertex) arg0);
		return null;
	}

	@Override
	public Edge getEdge(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Edge> getEdges() {

		return null;
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
			n = (EdbVertex) ByteArrayHelper.deserialize(edgeStore
					.get(ByteArrayHelper.serialize((Integer) arg0)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		mCache.put((Integer) arg0, n);

		return null;
	}

	@Override
	public Iterable<Vertex> getVertices() {

		return new EdbVertexIterator(new EdbCursor(edgeStore.getCursor()));
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeEdge(Edge arg0) {
		EdbEdge e2 = (EdbEdge) arg0;

		EdbVertex n = (EdbVertex) getVertex(e2.getToVertex());
		EdbEdgeIterator iterable = (EdbEdgeIterator) n.getEdges(Direction.OUT);
		Iterator<Edge> edges = iterable.iterator();
		while (edges.hasNext()) {
			EdbEdge ee = (EdbEdge) edges.next();
			if ((ee.getVertex(Direction.IN).equals(e2.getVertex(Direction.IN)))
					&& (ee.getRelation().equals(e2.getRelation())))
				edges.remove();
		}
		store(n);

		EdbVertex n2 = (EdbVertex) getVertex(e2.getVertex(Direction.IN));
		EdbEdgeIterator iterable2 = (EdbEdgeIterator) n2.getEdges(Direction.IN);
		Iterator<Edge> edges2 = iterable2.iterator();
		while (edges2.hasNext()) {
			EdbEdge ee = (EdbEdge) edges2.next();
			if ((ee.getToVertex().equals(e2.getToVertex()))
					&& (ee.getRelation().equals(e2.getRelation())))
				edges2.remove();
		}
		store(n2);

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

		for (Edge e : arg0.getEdges(Direction.BOTH, null)) {
			removeEdge(e);
		}

		mCache.remove((Integer) arg0.getId());
		edgeStore.delete(key);// remove(key);

	}

	@Override
	public void shutdown() {
		commit();
		edgeStore.close();
		edgeStore = null;

	}

	private void initStores(String path) {

		if (edgeStore == null) {
			edgeStore = new EdbKeyPairStore("edgeStore");

		}

	}

	protected void store(Vertex n) {
		try {
			edgeStore.put(ByteArrayHelper.serialize(n.getId()),
					ByteArrayHelper.serialize(n));
			edgeStore.sync();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void commit() {
		edgeStore.sync();

	}

}