package org.eulerdb.kernel;

import java.io.IOException;
import org.apache.log4j.*;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.iterator.EdbIterableFromDatabase;
import org.eulerdb.kernel.iterator.EdbIterableFromIterator;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EulerDBHelper;
import org.eulerdb.kernel.storage.EdbStorage.storeType;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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

	protected final Logger logger = Logger.getLogger(this.getClass().getCanonicalName());

	//protected boolean mTransactional;
	protected boolean mAutoIndex;
	protected EdbStorage mStorage = null;
	protected static EulerDBHelper mEdbHelper = null;
	protected boolean mIsRunning = false;

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
		
		logger.info("EulerDB is running in mode transactional: false, autoindex: false at path:" + path);
		
		if(mEdbHelper==null) 
			mEdbHelper = EulerDBHelper.getInstance(path, false);
		
		if (mStorage == null)
			mStorage = EdbStorage.getInstance(path, false,false);
		
		mIsRunning = true;
	}

	public EdbGraph(String path, boolean transactional, boolean autoIndex) {
		logger.info("EulerDB is running in mode transactional: "+transactional+", autoindex: "+ autoIndex+"at path:" + path);
		mAutoIndex = autoIndex;
		if(mEdbHelper==null) 
			mEdbHelper = EulerDBHelper.getInstance(path, transactional);
		
		if (mStorage == null)
			mStorage = EdbStorage.getInstance(path, transactional,autoIndex);
		
		mIsRunning = true;
	}

	@Override
	public Edge addEdge(Object id, Vertex n1, Vertex n2, String relation) {
		logger.debug("Adding Edge from Vertex "+ n1.getId()+" to Vertex "+n2.getId()+" with relation of "+ relation);
		EdbEdge e = new EdbEdge(n1, n2, id, relation);
		if(n1.equals(n2)){//self loop
			mStorage.store(storeType.EDGE, getTransaction(), (String)e.getId(),e);
			((EdbVertex) n1).addOutEdge(e);
			((EdbVertex) n1).addInEdge(e);
			mStorage.store(storeType.VERTEX, getTransaction(), (String)n1.getId(),n1);
		}
		else
		{
			mStorage.store(storeType.EDGE, getTransaction(),(String)e.getId(), e);
			((EdbVertex) n1).addOutEdge(e);
			((EdbVertex) n2).addInEdge(e);
			mStorage.store(storeType.VERTEX, getTransaction(),(String)n1.getId(), n1);// store(mNodePairs,n1);
			mStorage.store(storeType.VERTEX, getTransaction(),(String)n2.getId(), n2);// store(mNodePairs,n2);
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
		logger.debug("Adding vertex with id "+id);
		EdbVertex v = new EdbVertex(id);
		mStorage.store(storeType.VERTEX, getTransaction(),(String)v.getId(), v);

		return v;
	}

	@Override
	public Edge getEdge(Object arg0) {
		logger.debug("get edge with id" + arg0);
		if (arg0 == null)
			throw new IllegalArgumentException("Get id shouldn't be null");
		EdbEdge e = (EdbEdge) mStorage.getObj(storeType.EDGE, getTransaction(),
				String.valueOf(arg0));

		return e;
	}

	@Override
	public Iterable<Edge> getEdges() {
		logger.debug(" get edge iterable");
		return new EdbIterableFromDatabase(mStorage.getCursor(
				storeType.EDGE, getTransaction()));
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		logger.debug("get edge iterable with property key: "+arg0+" value of "+ arg1);
		final String key = arg0;
		final Object value = arg1;

		Predicate<Element> relationFilter = new Predicate<Element>() {
			public boolean apply(Element v) {
				if (null == v.getProperty(key))
					return false;
				else
					return v.getProperty(key).equals(value);
				// return true;
			}
		};

		Iterable<Edge> its = new EdbIterableFromIterator(Iterators
				.filter(new EdbIterableFromDatabase(
						mStorage.getCursor(storeType.EDGE, getTransaction())).iterator(),
						relationFilter));

		return its;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Vertex getVertex(Object arg0) {
		logger.debug("get vertex of id "+ arg0);
		if (arg0 == null)
			throw new IllegalArgumentException("argument is null");
		EdbVertex n = (EdbVertex) mStorage.getObj(storeType.VERTEX, getTransaction(),
				String.valueOf(arg0));

		return n;
	}

	@Override
	public Iterable<Vertex> getVertices() {
		logger.debug(" get vertex iterable");
		return new EdbIterableFromDatabase(mStorage.getCursor(
				storeType.VERTEX, getTransaction()));
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		logger.debug("get vertex iterable with property key: "+arg0+" value of "+ arg1);
		final String key = arg0;
		final Object value = arg1;

		Predicate<Element> relationFilter = new Predicate<Element>() {
			public boolean apply(Element v) {
				if (null == v.getProperty(key))
					return false;
				else
					return v.getProperty(key).equals(value);
				// return true;
			}
		};
		
		Iterable<Vertex> its = new EdbIterableFromIterator(Iterators
				.filter(new EdbIterableFromDatabase(mStorage.getCursor(
						storeType.VERTEX, getTransaction()
						)).iterator(), relationFilter));

		return its;
	}

	@Override
	public void removeEdge(Edge arg0) {
		logger.debug(" remove edge of id "+ arg0.getId());
		EdbEdge e2 = (EdbEdge) arg0;

		EdbVertex n = (EdbVertex) e2.getVertex(Direction.OUT);
		if (n!=null) {
			// check whether vertex still exist, we might have remove vertex before the edge
			n.removeOutEdge(e2);
			mStorage.store(storeType.VERTEX, getTransaction(),(String)n.getId(), n);
		}

		EdbVertex n2 = (EdbVertex) e2.getVertex(Direction.IN);
		if (n2!=null) {
			n2.removeInEdge(e2);
			mStorage.store(storeType.VERTEX, getTransaction(),(String)n2.getId(), n2);
		}

		mStorage.delete(storeType.EDGE, getTransaction(), e2.getId());

	}

	@Override
	public void removeVertex(Vertex arg0) {
		logger.debug(" remove vertex of id"+ arg0.getId());
		byte[] key = null;
		try {
			key = ByteArrayHelper.serialize((String) arg0.getId());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (Edge e : arg0.getEdges(Direction.IN)) {
			removeEdge(e);
		}

		for (Edge e : arg0.getEdges(Direction.OUT)) {
			removeEdge(e);
		}

		mStorage.delete(storeType.VERTEX, getTransaction(), arg0.getId());// remove(key);

	}

	@Override
	public void shutdown() {
		logger.info("Shutting down the EulerDB");
		// nontransactionalCommit();
		if(mIsRunning){
			mStorage.close();
			mStorage = null;
			mEdbHelper = null;
			mIsRunning = false;
		}
	}

	public void nontransactionalCommit() {
		logger.info("nontransactionalCommit");
		mStorage.commit();
	}
	
	protected Transaction getTransaction(){
		logger.debug("getTransaction");
		return null;
	}

}