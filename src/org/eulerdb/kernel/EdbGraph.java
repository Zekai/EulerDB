package org.eulerdb.kernel;

import java.io.IOException;
import java.util.Iterator;
import org.apache.log4j.*;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.iterator.IteratorFactory;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EulerDBHelper;
import org.eulerdb.kernel.storage.EdbStorage.storeType;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Model https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model
 * 
 * @author Zekai Huang
 * 
 *         check this out for ACID
 *         http://www.fredosaurus.com/notes-db/transactions/acid.html
 */

public class EdbGraph implements Graph {

	private static final Logger logger = Logger.getLogger(EdbGraph.class
			.getCanonicalName());

	protected boolean mTransactional;
	protected static Transaction mTx = null;
	protected EdbStorage mStorage = null;
	protected static EulerDBHelper mEdbHelper = null;

	protected static final Features FEATURES = new Features();

	static {
		FEATURES.supportsDuplicateEdges = false; // duplicated edge mean
													// completely duplicated
		FEATURES.supportsSelfLoops = true;
		FEATURES.isPersistent = true;
		FEATURES.isRDFModel = false;
		FEATURES.supportsVertexIteration = true;
		FEATURES.supportsEdgeIteration = true;
		FEATURES.supportsVertexIndex = false;
		FEATURES.supportsEdgeIndex = false;
		FEATURES.ignoresSuppliedIds = true;
		FEATURES.supportsTransactions = false;
		FEATURES.supportsEdgeKeyIndex = false;
		FEATURES.supportsVertexKeyIndex = false;
		FEATURES.supportsKeyIndices = false;
		FEATURES.isWrapper = false;
		FEATURES.supportsIndices = false;

		FEATURES.supportsSerializableObjectProperty = true;
		FEATURES.supportsBooleanProperty = true;
		FEATURES.supportsDoubleProperty = true;
		FEATURES.supportsFloatProperty = true;
		FEATURES.supportsIntegerProperty = true;
		FEATURES.supportsPrimitiveArrayProperty = true;
		FEATURES.supportsUniformListProperty = true;
		FEATURES.supportsMixedListProperty = true;
		FEATURES.supportsLongProperty = true;
		FEATURES.supportsMapProperty = true;
		FEATURES.supportsStringProperty = true;
		FEATURES.supportsThreadedTransactions = true;
	}

	public EdbGraph(String path) {
		mTransactional = false;
		if(mEdbHelper==null) 
			mEdbHelper = EulerDBHelper.getInstance(path, false);
		
		if (mStorage == null)
			mStorage = EdbStorage.getInstance(path, false);
	}

	public EdbGraph(String path, boolean transactional) {
		mTransactional = transactional;
		
		if(mEdbHelper==null) 
			mEdbHelper = EulerDBHelper.getInstance(path, transactional);
		
		if (mStorage == null)
			mStorage = EdbStorage.getInstance(path, mTransactional);
	}

	@Override
	public Edge addEdge(Object id, Vertex n1, Vertex n2, String relation) {

		EdbEdge e = new EdbEdge(n1, n2, id, relation);
		if(n1.equals(n2)){//self loop
			mStorage.store(storeType.EDGE, mTx, e);
			((EdbVertex) n1).addOutEdge(e);
			((EdbVertex) n1).addInEdge(e);
			mStorage.store(storeType.VERTEX, mTx, n1);
		}
		else
		{
			mStorage.store(storeType.EDGE, mTx, e);
			((EdbVertex) n1).addOutEdge(e);
			((EdbVertex) n2).addInEdge(e);
			mStorage.store(storeType.VERTEX, mTx, n1);// store(mNodePairs,n1);
			mStorage.store(storeType.VERTEX, mTx, n2);// store(mNodePairs,n2);
		}
		return e;
	}

	/**
	 * Object arg0: vertex
	 */
	/*
	 * @Override public Vertex addVertex(Object arg0) { store(mNodePairs,
	 * (EdbVertex)arg0); mCache.put((Integer) ((EdbVertex) arg0).getId(),
	 * (EdbVertex) arg0); return (EdbVertex)arg0; }
	 */

	@Override
	public Vertex addVertex(Object id) {
		EdbVertex v = new EdbVertex(id);
		mStorage.store(storeType.VERTEX, mTx, v);

		return v;
	}

	@Override
	public Edge getEdge(Object arg0) {
		if (arg0 == null)
			throw new IllegalArgumentException("Get id shouldn't be null");
		EdbEdge e = (EdbEdge) mStorage.getObj(storeType.EDGE, mTx,
				String.valueOf(arg0));

		return e;
	}

	@Override
	public Iterable<Edge> getEdges() {

		return IteratorFactory.getEdgeIterator(mStorage.getCursor(
				storeType.EDGE, mTx));
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;

		Predicate<Edge> relationFilter = new Predicate<Edge>() {
			public boolean apply(Edge v) {
				if (null == v.getProperty(key))
					return false;
				else
					return v.getProperty(key).equals(value);
				// return true;
			}
		};

		Iterable<Edge> its = IteratorFactory.getEdgeIterator(Iterators
				.filter(IteratorFactory.getEdgeIterator(
						mStorage.getCursor(storeType.EDGE, mTx)).iterator(),
						relationFilter));

		return its;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Vertex getVertex(Object arg0) {
		if (arg0 == null)
			throw new IllegalArgumentException("argument is null");
		EdbVertex n = (EdbVertex) mStorage.getObj(storeType.VERTEX, mTx,
				String.valueOf(arg0));

		return n;
	}

	@Override
	public Iterable<Vertex> getVertices() {

		return IteratorFactory.getVertexIterator(mStorage.getCursor(
				storeType.VERTEX, mTx));
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;

		Predicate<Vertex> relationFilter = new Predicate<Vertex>() {
			public boolean apply(Vertex v) {
				if (null == v.getProperty(key))
					return false;
				else
					return v.getProperty(key).equals(value);
				// return true;
			}
		};

		/*Iterable<Vertex> its = 
				IteratorFactory.getVertexIterator(
						Iterators.filter(
								IteratorFactory.getVertexIterator(
										mStorage.getCursor(
												storeType.VERTEX, mTx
												)
										).iterator(), relationFilter
								)
						);*/
		
		Iterable<Vertex> its = IteratorFactory.getVertexIterator(Iterators
				.filter(IteratorFactory.getVertexIterator(mStorage.getCursor(
						storeType.VERTEX, mTx
						)).iterator(), relationFilter));

		return its;
	}

	@Override
	public void removeEdge(Edge arg0) {
		EdbEdge e2 = (EdbEdge) arg0;

		EdbVertex n = (EdbVertex) e2.getVertex(Direction.OUT);
		if (mStorage.containsKey(storeType.VERTEX, mTx, n.getId())) {// check
																		// whether
																		// vertex
																		// still
																		// exist,
																		// we
																		// might
																		// have
																		// remove
																		// vertex
																		// before
																		// the
																		// edge
			n.removeOutEdge(e2);
			mStorage.store(storeType.VERTEX, mTx, n);
		}

		EdbVertex n2 = (EdbVertex) e2.getVertex(Direction.IN);
		if (mStorage.containsKey(storeType.VERTEX, mTx, n2.getId())) {// check
																		// whether
																		// vertex
																		// still
																		// exist,
																		// we
																		// might
																		// have
																		// remove
																		// vertex
																		// before
																		// the
																		// edge
			n2.removeInEdge(e2);
			mStorage.store(storeType.VERTEX, mTx, n2);
		}

		mStorage.delete(storeType.EDGE, mTx, e2);

	}

	@Override
	public void removeVertex(Vertex arg0) {
		byte[] key = null;
		try {
			key = ByteArrayHelper.serialize((String) arg0.getId());
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

		mStorage.delete(storeType.VERTEX, mTx, arg0);// remove(key);

	}

	@Override
	public void shutdown() {
		// nontransactionalCommit();
		mStorage.close();
		mTx = null;
		mStorage = null;
		mEdbHelper = null;
	}

	public void nontransactionalCommit() {
		mStorage.commit();
	}

}