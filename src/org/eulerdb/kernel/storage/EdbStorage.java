package org.eulerdb.kernel.storage;

import java.io.IOException;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Element;

/**
 * Why this can be a singleton even while we are supporting multiple isolated EdbGraph instant and transactions?
 * We don't really implement the low level ACID, instead, the BerkeleyDB simulate it. 
 * @author Zekai Huang
 *
 */
public class EdbStorage {
	private static EdbStorage instance = null;
	
	public static enum storeType {VERTEX,EDGE};
	private static EdbKeyPairStore mNodePairs;
	private static EdbKeyPairStore mEdgePairs;
	private static EdbCaching mCache;
	private static EulerDBHelper mEdbHelper = null;
	private static boolean mTransactional;
	private static EdbCursor mEdgeCursor;
	private static EdbCursor mNodeCursor;
	
	private EdbStorage(String path){
		mCache = EdbCaching.getInstance();
		initStores(path);
	}

	public static EdbStorage getInstance(String path,boolean transactional) {
		mTransactional = transactional;
		if (instance == null) {
			instance = new EdbStorage(path);
		}
		return instance;
	}

	public static EdbStorage getInstance() {
		if(instance==null)
		{
			throw new IllegalArgumentException("EdbGraph.class needs to pass in the path, use getInstance(String path) instead");
		}
		return instance;
	}
	
	private void initStores(String path) {
		if(mEdbHelper == null) {
			mEdbHelper = EulerDBHelper.getInstance(path,mTransactional);
		}
		if (mNodePairs == null) {
			mNodePairs = new EdbKeyPairStore(mEdbHelper,Common.VERTEXSTORE);
		}
		
		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(mEdbHelper,Common.EDGESTORE);
		}

	}
	
	private EdbKeyPairStore getStore(storeType type){
		switch(type){
		case VERTEX:
			return mNodePairs;
		case EDGE:
			return mEdgePairs;
		default:
			throw new IllegalArgumentException("Type "+ type +" is unknown.");
		}
	}
	
	public void store(storeType type,Transaction tx,Element n) {
		try {
			getStore(type).put(tx,ByteArrayHelper.serialize(n.getId()),
					ByteArrayHelper.serialize(n));
			//getCursor(type,tx);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(type==storeType.VERTEX){
			mCache.put((String)n.getId(),getTransactionId(tx), (EdbVertex)n);
		}
	}
	
	public Long getTransactionId(Transaction tx){
		return tx==null?0:tx.getId();
	}
	
	public void delete(storeType type,Transaction tx,Element o){
		try {
			getStore(type).delete(tx,ByteArrayHelper.serialize(o.getId()));
			//getCursor(type,tx);
			if(type == storeType.VERTEX) 
				mCache.remove((String)o.getId(),getTransactionId(tx));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Object getObj(storeType type,Transaction tx, String id){
		Object o = mCache.get(id, getTransactionId(tx));
		if(o!=null) return o;
		
		try {
			o = ByteArrayHelper.deserialize(getStore(type).get(tx, ByteArrayHelper.serialize(id)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(type == storeType.VERTEX) mCache.put(id, getTransactionId(tx), (EdbVertex) o);
		
		return o;
	}
	
	public EdbCursor getCursor(storeType type,Transaction tx) {
		switch(type)
		{
		case EDGE:
		{
			if(mEdgeCursor!=null) mEdgeCursor.close();
			mEdgeCursor = new EdbCursor(getStore(type).getCursor(tx));
			return mEdgeCursor;
		}
		case VERTEX:
		{
			if(mNodeCursor!=null) mNodeCursor.close();
			mNodeCursor = new EdbCursor(getStore(type).getCursor(tx));
			return mNodeCursor;
		}
		default:
			return null;
			
		} 
	}
	
	public boolean containsKey(storeType type,Transaction tx,String key) {
		try {
			if(getStore(type).get(tx, ByteArrayHelper.serialize(key))!=null)
				return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
	public void closeCursor(){
		if(mEdgeCursor!=null){
			mEdgeCursor.close();
			mEdgeCursor = null;
		}
		if(mNodeCursor!=null){
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
		mCache = null;
		instance = null;
	}
	
	public void commit() {
		mNodePairs.sync();
		mEdgePairs.sync();
	}
	
	public void resetCache(Transaction tx){
		mCache.clear(getTransactionId(tx));//FIXME shoudn't clear all, should clear for transaction, use region
	}


}
