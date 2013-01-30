package org.eulerdb.kernel.storage;

import java.io.IOException;
import java.util.Set;

import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

/**
 * Why this can be a singleton even while we are supporting multiple isolated EdbGraph instant and transactions?
 * We don't really implement the low level ACID, instead, we simulate it by using BerkeleyDB's transaction. 
 * @author Zekai Huang
 *
 */
public class EdbStorage {
	private static EdbStorage instance = null;
	
	public static enum storeType {VERTEX,EDGE,VERTEX_OUT,VERTEX_IN,PROPERTY,NODEPROPERTY,EDGEPROPERTY};
	private static EdbKeyPairStore mNodePairs;
	private static EdbKeyPairStore mEdgePairs;
	private static EdbKeyPairStore mNodeOutPairs;
	private static EdbKeyPairStore mNodeInPairs;
	private static EdbKeyPairStore mNodePropertyPairs;
	private static EdbKeyPairStore mEdgePropertyPairs;
	private static EulerDBHelper mEdbHelper = null;
	private static boolean mTransactional;
	private static EdbCursor mEdgeCursor;
	private static EdbCursor mNodeCursor;
	
	private EdbStorage(String path){
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
			mNodePairs = new EdbKeyPairStore(mEdbHelper,Common.VERTEXSTORE,false);
		}
		
		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(mEdbHelper,Common.EDGESTORE,false);
		}
		
		if (mNodeOutPairs == null) {
			mNodeOutPairs = new EdbKeyPairStore(mEdbHelper,Common.VERTEXOUTSTORE,false);
		}
		
		if (mNodeInPairs == null) {
			mNodeInPairs = new EdbKeyPairStore(mEdbHelper,Common.VERTEXINSTORE,false);
		}
		
		if (mNodePropertyPairs == null) {
			mNodePropertyPairs = new EdbKeyPairStore(mEdbHelper,Common.PROPERTY,true);
		}
		
		if (mEdgePropertyPairs == null) {
			mEdgePropertyPairs = new EdbKeyPairStore(mEdbHelper,Common.PROPERTY,true);
		}

	}
	
	private EdbKeyPairStore getStore(storeType type){
		switch(type){
		case VERTEX:
			return mNodePairs;
		case EDGE:
			return mEdgePairs;
		case VERTEX_OUT:
			return mNodeOutPairs;
		case VERTEX_IN:
			return mNodeInPairs;
		case NODEPROPERTY:
			return mNodePropertyPairs;
		case EDGEPROPERTY:
			return mEdgePropertyPairs;
		default:
			throw new IllegalArgumentException("Type "+ type +" is unknown.");
		}
	}
	
	public void store(storeType type,Transaction tx,String id,Object n) {
		try {
			switch(type){
			case VERTEX:
			case EDGE:
			case VERTEX_OUT:
			case VERTEX_IN:
				getStore(type).put(tx,ByteArrayHelper.serialize(id),
				ByteArrayHelper.serialize(n));
				break;
			case NODEPROPERTY:
			case EDGEPROPERTY:
				DatabaseEntry theKey = new DatabaseEntry(ByteArrayHelper.serialize(id));
			    DatabaseEntry theData = new DatabaseEntry();
				PropertyBinding dataBinding = new PropertyBinding();
				dataBinding.objectToEntry(n, theData);
				getStore(type).put(tx, theKey, theData);
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public Long getTransactionId(Transaction tx){
		return tx==null?0:tx.getId();
	}
	
	public void delete(storeType type,Transaction tx,Object id){
		try {
			getStore(type).delete(tx,ByteArrayHelper.serialize(id));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Object getObj(storeType type, Transaction tx, String id) {
		Object o = null; // mCache.get(id, tx.getId());
		
		if (type != storeType.PROPERTY) {
			try {
				o = ByteArrayHelper.deserialize(getStore(type).get(tx,
						ByteArrayHelper.serialize(id)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			DatabaseEntry theKey = null;
			try {
				theKey = new DatabaseEntry(ByteArrayHelper.serialize(id));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    DatabaseEntry theData = getStore(type).get(tx,theKey);
					
			PropertyBinding dataBinding = new PropertyBinding();
			o = dataBinding.entryToObject(theData);
		}

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
		mNodeOutPairs.close();
		mNodeOutPairs = null;
		mNodeInPairs.close();
		mNodeInPairs = null;
		mNodePropertyPairs.close();
		mNodePropertyPairs = null;
		mEdgePropertyPairs.close();
		mEdgePropertyPairs = null;
		mEdbHelper.closeEnv();
		mEdbHelper = null;
		instance = null;
	}
	
	public void commit() {
		mNodeOutPairs.sync();
		mNodeInPairs.sync();
		mNodePropertyPairs.sync();
		mEdgePropertyPairs.sync();
		mNodePairs.sync();
		mEdgePairs.sync();
	}
	
	public void openSecondary(String key, storeType type){
		getStore(type).openSecondary(key);
	}
	
	public Set<String> getKeys(storeType type){
		return getStore(type).getKeys();
	}
	public void deleteSecondary(storeType type,Transaction tx,String dbName){
		getStore(type).deleteSecondary(tx, dbName);
	}
	

}
