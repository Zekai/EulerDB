package org.eulerdb.kernel.storage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbHelper;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * Why this can be a singleton even while we are supporting multiple isolated
 * EdbGraph instant and transactions? We don't really implement the low level
 * ACID, instead, the BerkeleyDB simulate it.
 * 
 * @author Zekai Huang
 * 
 */
public class EdbStorage {
	private static EdbStorage instance = null;

	public static enum storeType {
		VERTEX, EDGE
	};

	private static EdbKeyPairStore mNodePairs;
	private static EdbKeyPairStore mEdgePairs;
	private static LoadingCache<String, Optional<Vertex>> mVertexCache;
	private static LoadingCache<String, Optional<Edge>> mEdgeCache;
	private static EulerDBHelper mEdbHelper = null;
	private static boolean mTransactional;
	private static EdbCursor mEdgeCursor;
	private static EdbCursor mNodeCursor;

	private EdbStorage(String path) {

		initStores(path);
		initCache();
	}

	public static EdbStorage getInstance(String path, boolean transactional) {
		mTransactional = transactional;
		if (instance == null) {
			instance = new EdbStorage(path);
		}
		return instance;
	}

	public static EdbStorage getInstance() {
		if (instance == null) {
			throw new IllegalArgumentException(
					"EdbGraph.class needs to pass in the path, use getInstance(String path) instead");
		}
		return instance;
	}

	private void initStores(String path) {
		if (mEdbHelper == null) {
			mEdbHelper = EulerDBHelper.getInstance(path, mTransactional);
		}
		if (mNodePairs == null) {
			mNodePairs = new EdbKeyPairStore(mEdbHelper, Common.VERTEXSTORE);
		}

		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(mEdbHelper, Common.EDGESTORE);
		}

	}

	private void initCache() {
		CacheLoader<String, Optional<Vertex>> vertexLoader = new CacheLoader<String, Optional<Vertex>>() {
			public Optional<Vertex> load(String key) {
				String id = getRealId(key);
				System.out.println("real "+ id);
				Vertex o = null;
				try {
					o = (Vertex) ByteArrayHelper.deserialize(getStore(
							storeType.VERTEX).get(EdbTransactionalGraph.txs.get(),
							ByteArrayHelper.serialize(id)));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return Optional.fromNullable(o);
			}
		};
		mVertexCache = CacheBuilder.newBuilder()
				.softValues()
				.build(vertexLoader);

		CacheLoader<String, Optional<Edge>> edgeLoader = new CacheLoader<String, Optional<Edge>>() {
			public Optional<Edge> load(String key) {
				String id = getRealId(key);
				Edge o = null;
				try {
					o = (Edge) ByteArrayHelper.deserialize(getStore(
							storeType.EDGE).get(EdbTransactionalGraph.txs.get(),
							ByteArrayHelper.serialize(id)));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return Optional.fromNullable(o);
			}
		};
		mEdgeCache = CacheBuilder.newBuilder()
				.softValues()
				.build(edgeLoader);
	}

	private EdbKeyPairStore getStore(storeType type) {
		switch (type) {
		case VERTEX:
			return mNodePairs;
		case EDGE:
			return mEdgePairs;
		default:
			throw new IllegalArgumentException("Type " + type + " is unknown.");
		}
	}

	private LoadingCache getCache(storeType type) {
		switch (type) {
		case VERTEX:
			return mVertexCache;
		case EDGE:
			return mEdgeCache;
		default:
			throw new IllegalArgumentException("Type " + type + " is unknown.");
		}
	}

	public void store(storeType type, Transaction tx, Element n) {
		try {
			getStore(type).put(tx, ByteArrayHelper.serialize(n.getId()),
					ByteArrayHelper.serialize(n));
			// getCursor(type,tx);
		} catch (IOException e) {
			e.printStackTrace();
		}
		getCache(type).put(getCacheId(EdbHelper.getTransactionId(tx),(String) n.getId()), Optional.fromNullable(n));
	}

	public void delete(storeType type, Transaction tx, Element o) {
		try {
			getStore(type).delete(tx, ByteArrayHelper.serialize(o.getId()));
			// mCache.remove((String)o.getId(),EdbHelper.getTransactionId(tx));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		getCache(type).invalidate(getCacheId(EdbHelper.getTransactionId(tx),(String)o.getId()));
	}

	public Object getObj(storeType type, Transaction tx, String id) {
		Optional<Object> o = null;
		try {
			o = (Optional<Object>) getCache(type).get(getCacheId(EdbHelper.getTransactionId(tx),id));
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		if (o.isPresent())
			return o.get();
		else
			return null;
	}

	public EdbCursor getCursor(storeType type, Transaction tx) {
		switch (type) {
		case EDGE: {
			if (mEdgeCursor != null)
				mEdgeCursor.close();
			mEdgeCursor = new EdbCursor(type, getStore(type).getCursor(tx), tx);
			return mEdgeCursor;
		}
		case VERTEX: {
			if (mNodeCursor != null)
				mNodeCursor.close();
			mNodeCursor = new EdbCursor(type, getStore(type).getCursor(tx), tx);
			return mNodeCursor;
		}
		default:
			return null;

		}
	}
/*
	public boolean containsKey(storeType type, Transaction tx, String key) {
		try {
			if (getStore(type).get(tx, ByteArrayHelper.serialize(key)) != null)
				return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
*/
	public void closeCursor() {
		if (mEdgeCursor != null) {
			mEdgeCursor.close();
			mEdgeCursor = null;
		}
		if (mNodeCursor != null) {
			mNodeCursor.close();
			mNodeCursor = null;
		}
	}

	public void close() {
		closeCursor();
		mNodePairs.close();
		mEdgePairs.close();
		mNodePairs = null;
		mEdgePairs = null;
		mEdbHelper.closeEnv();
		mEdbHelper = null;
		mVertexCache.cleanUp();
		mVertexCache = null;
		mEdgeCache.cleanUp();
		mEdgeCache = null;
		instance = null;
	}

	public void commit() {
		mNodePairs.sync();
		mEdgePairs.sync();
	}

	public void resetCache(Transaction tx) {
		mVertexCache.cleanUp();// FIXME shoudn't clear all, should clear for
								// transaction, use region
		mEdgeCache.cleanUp();
	}
	
	private String getCacheId(Long tid,String id){
		return tid+Common.SEPARATOR_CAHCEID+id;
	}
	
	private String getRealId(String cacheId){
		return cacheId.substring(cacheId.indexOf(Common.SEPARATOR_CAHCEID)+1);
	}

}
