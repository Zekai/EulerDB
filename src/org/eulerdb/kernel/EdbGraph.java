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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
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
 * @author Zekai Huang
 *
 * check this out for ACID
 * http://www.fredosaurus.com/notes-db/transactions/acid.html
 */

public class EdbGraph implements Graph {

	private static final Logger logger = Logger.getLogger(EdbGraph.class
			.getCanonicalName());

	public EdbKeyPairStore mNodePairs;
	public EdbKeyPairStore mEdgePairs;
	
	protected boolean mTransactional;
	protected Transaction mTx = null;

	protected EdbCaching mCache;
	
	protected EulerDBHelper mEdbHelper = null;
	
	protected static final Features FEATURES = new Features();

	static {
		FEATURES.supportsDuplicateEdges = false; //duplicated edge mean completely duplicated
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
		mCache = EdbCaching.getInstance();
		
		initStores(path);
	}

	public EdbGraph(String path,boolean transactional) {
		mTransactional = transactional;
		mCache = EdbCaching.getInstance();
		
		initStores(path);
	}

	@Override
	public Edge addEdge(Object id, Vertex n1, Vertex n2, String relation) {
		
		EdbEdge e = new EdbEdge(n1, n2, id, relation);
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
	/*@Override
	public Vertex addVertex(Object arg0) {
		store(mNodePairs, (EdbVertex)arg0);
		mCache.put((Integer) ((EdbVertex) arg0).getId(), (EdbVertex) arg0);
		return (EdbVertex)arg0;
	}*/
	
	@Override
	public Vertex addVertex(Object id) {
		EdbVertex v = new EdbVertex((String)id);
		store(mNodePairs, v);
		mCache.put((String)id, v);
		return v;
	}

	@Override
	public Edge getEdge(Object arg0) {
		if(arg0==null) throw new IllegalArgumentException("Get id shouldn't be null");
		EdbEdge e = null;
		try {
			e = (EdbEdge) ByteArrayHelper.deserialize(mEdgePairs.get(mTx,ByteArrayHelper.serialize(arg0)));
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

		 return IteratorFactory.getEdgeIterator(mEdgePairs.getCursor(mTx));
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {

		
		return null;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Vertex getVertex(Object arg0) {
		if(arg0==null) throw new IllegalArgumentException("argument is null");
		EdbVertex n = null;//mCache.get((Integer) arg0);
		if (n != null)
			return n;

		try {
			n = (EdbVertex) ByteArrayHelper.deserialize(mNodePairs
					.get(mTx,ByteArrayHelper.serialize(String.valueOf(arg0))));
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			logger.error(e);
			e.printStackTrace();
		}

		mCache.put(String.valueOf(arg0), n);

		return n;
	}

	@Override
	public Iterable<Vertex> getVertices() {
		
		 return IteratorFactory.getVertexIterator(mNodePairs.getCursor(mTx));
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0,  Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;

		Predicate<Vertex> relationFilter = new Predicate<Vertex>() {
			public boolean apply(Vertex v) {
				if(null==v.getProperty(key))
					return false;
				else 
					return v.getProperty(key).equals(value);
				//return true;
			}
		};

		Iterable<Vertex> its = IteratorFactory.getVertexIterator(Iterators
				.filter(IteratorFactory.getVertexIterator(mNodePairs
						.getCursor(mTx)), relationFilter));

		return its;
	}

	@Override
	public void removeEdge(Edge arg0) {
		EdbEdge e2 = (EdbEdge) arg0;

		EdbVertex n = (EdbVertex) e2.getVertex(Direction.OUT);
		try {
			if(mNodePairs.get(mTx, ByteArrayHelper.serialize(n.getId()))!=null)
			{//check whether vertex still exist, we might have remove vertex before the edge
				n.removeOutEdge(e2);
				store(mNodePairs, n);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		EdbVertex n2 = (EdbVertex) e2.getVertex(Direction.IN);
		try {
			if(mNodePairs.get(mTx, ByteArrayHelper.serialize(n.getId()))!=null)
			{//check whether vertex still exist, we might have remove vertex before the edge
				n2.removeInEdge(e2);
				store(mNodePairs, n2);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		try {
			mEdgePairs.delete(mTx,ByteArrayHelper.serialize(e2.getId()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

		mCache.remove((String) arg0.getId());
		mNodePairs.delete(mTx,key);// remove(key);

	}

	@Override
	public void shutdown() {
		//nontransactionalCommit();
		mNodePairs.close();
		mEdgePairs.close();
		mNodePairs = null;
		mEdgePairs = null;
		mEdbHelper.getEnvironment().close();

	}

	private void initStores(String path) {
		if(mEdbHelper == null) {
			mEdbHelper = new EulerDBHelper(path,mTransactional);
		}
		if (mNodePairs == null) {
			mNodePairs = new EdbKeyPairStore(mEdbHelper,Common.VERTEXSTORE);
		}
		
		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(mEdbHelper,Common.EDGESTORE);
		}

	}
	
	protected void store(EdbKeyPairStore store,Element n) {
		try {
			store.put(mTx,ByteArrayHelper.serialize(n.getId()),
					ByteArrayHelper.serialize(n));
			//store.sync();//shouldn't commit here, should be done when implementing the interface for transactional graph
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void nontransactionalCommit() {
		mNodePairs.sync();
		mEdgePairs.sync();
	}

}