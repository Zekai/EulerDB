package org.eulerdb.kernel.storage;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.FileHelper;
import org.eulerdb.kernel.iterator.EdbPrimaryCursor;
import org.eulerdb.kernel.iterator.EdbSecondaryCursor;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

/**
 * Why this can be a singleton even while we are supporting multiple isolated EdbGraph instant and transactions?
 * We don't really implement the low level ACID, instead, we simulate it by using BerkeleyDB's transaction. 
 * @author Zekai Huang
 *
 */
public class EdbStorage {
	
	private  Logger logger = Logger.getLogger(EdbStorage.class.getCanonicalName());
	
	public  enum storeType {VERTEX,EDGE,VERTEX_OUT,VERTEX_IN,NODEPROPERTY,EDGEPROPERTY};
	private  EdbKeyPairStore mNodePairs;
	private  EdbKeyPairStore mEdgePairs;
	private  EdbKeyPairStore mNodeOutPairs;
	private  EdbKeyPairStore mNodeInPairs;
	private  EdbKeyPairStore mNodePropertyPairs;
	private  EdbKeyPairStore mEdgePropertyPairs;
	//private  boolean mTransactional;
	private  EdbPrimaryCursor mEdgeCursor;
	private  EdbPrimaryCursor mNodeCursor;
	private  EdbSecondaryCursor mEdgePropCursor;
	private  EdbSecondaryCursor mNodePropCursor;
	
	private Environment mEnv;
	
	EdbStorage(String path,boolean transactional,boolean autoindex){
		initStores(path,transactional,autoindex);
		logger.debug("initStores in mode transactional: "+transactional+", autoindex: "+ autoindex+"at path:" + path);
	}
	
	public Transaction beginTransaction(){
		return mEnv.beginTransaction(null, null);
	}

	/*
	public  synchronized EdbStorage getInstance(String path,boolean transactional,boolean autoindex) {
		if (instance == null) {
			instance = new EdbStorage(path,transactional,autoindex);
		}
		return instance;
	}*/
	
	private void initEnv(String name,boolean isTransactional){
		EnvironmentConfig envConf = new EnvironmentConfig();
		// environment will be created if not exists
		File f = new File(FileHelper.appendFileName(EdbManager.getBase(),name));
		if (!f.exists())
		{
			f.mkdirs();
		}
		if(!isTransactional){
			envConf.setAllowCreate(true);
		}
		else
		{
			envConf.setAllowCreate(true);
			envConf.setTransactional(true);
		}
		mEnv = new Environment(f, envConf);
	}
	
	private void initStores(String name,boolean isTransactional,boolean autoIndex) {
		initEnv(name,isTransactional);
		DatabaseConfig dbConf = new DatabaseConfig();
		dbConf.setAllowCreate(true);
		if (!isTransactional) {
			dbConf.setDeferredWrite(true);
			// dbConf.setSortedDuplicates(true);
		}else {
			dbConf.setTransactional(true);
			dbConf.setKeyPrefixing(true);
		}
		
		if (mNodePairs == null) {
			mNodePairs = new EdbKeyPairStore(mEnv,dbConf,Common.VERTEXSTORE,false);
		}
		
		if (mEdgePairs == null) {
			mEdgePairs = new EdbKeyPairStore(mEnv,dbConf,Common.EDGESTORE,false);
		}
		
		if (mNodeOutPairs == null) {
			mNodeOutPairs = new EdbKeyPairStore(mEnv,dbConf,Common.VERTEXOUTSTORE,false);
		}
		
		if (mNodeInPairs == null) {
			mNodeInPairs = new EdbKeyPairStore(mEnv,dbConf,Common.VERTEXINSTORE,false);
		}
		
		if (mNodePropertyPairs == null) {
			mNodePropertyPairs = new EdbKeyPairStore(mEnv,dbConf,Common.VERTEXPROPERTY,autoIndex);
		}
		
		if (mEdgePropertyPairs == null) {
			mEdgePropertyPairs = new EdbKeyPairStore(mEnv,dbConf,Common.EDGEPROPERTY,autoIndex);
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
	
	public long getCount(storeType type){
		return getStore(type).count();
	}
	
	public void store(storeType type,Transaction tx,String id,Object n) {
		try {
			
				getStore(type).put(tx,ByteArrayHelper.serialize(id),
				ByteArrayHelper.serialize(n));
				
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
		
		/*switch(type){
		case VERTEX:
		case EDGE:
		case VERTEX_OUT:
		case VERTEX_IN:
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
			break;
		case NODEPROPERTY:
		case EDGEPROPERTY:
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
			break;
		}*/
		
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
		return o;
	}
	
	public EdbPrimaryCursor getCursor(storeType type,Transaction tx) {
		switch(type)
		{
		case EDGE:
		{
			if(mEdgeCursor!=null) mEdgeCursor.close();
			mEdgeCursor = new EdbPrimaryCursor(getStore(type).getCursor(tx));
			return mEdgeCursor;
		}
		case VERTEX:
		{
			if(mNodeCursor!=null) mNodeCursor.close();
			mNodeCursor = new EdbPrimaryCursor(getStore(type).getCursor(tx));
			return mNodeCursor;
		}
		default:
			return null;
			
		} 
	}
	
	public EdbSecondaryCursor getSecondaryCursor(storeType type,Transaction tx,String pKey,Object pValue) {
		
		switch(type)
		{
		case EDGEPROPERTY:
		{
			if(mEdgePropCursor!=null) mEdgePropCursor.close();
			mEdgePropCursor = new EdbSecondaryCursor(getStore(type).getSecondCursor(pKey,tx),pValue);
			return mEdgePropCursor;
		}
		case NODEPROPERTY:
		{
			if(mNodePropCursor!=null) mNodePropCursor.close();
			mNodePropCursor = new EdbSecondaryCursor(getStore(type).getSecondCursor(pKey,tx),pValue);
			return mNodePropCursor;
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
	public synchronized void closeCursor(){
		if(mEdgeCursor!=null){
			mEdgeCursor.close();
			mEdgeCursor = null;
		}
		if(mNodeCursor!=null){
			mNodeCursor.close();
			mNodeCursor = null;
		}
		if(mEdgePropCursor!=null){
			mEdgePropCursor.close();
			mEdgePropCursor = null;
		}
		if(mNodePropCursor!=null){
			mNodePropCursor.close();
			mNodePropCursor = null;
		}
	}
	public synchronized void close() {
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
		mEnv.close();
		mEnv = null;
	}
	
	public void commit() {
		mNodeOutPairs.sync();
		mNodeInPairs.sync();
		mNodePropertyPairs.sync();
		mEdgePropertyPairs.sync();
		mNodePairs.sync();
		mEdgePairs.sync();
	}
	
	/*
	public void openSecondary(String key, storeType type){
		getStore(type).openSecondary(key);
	}*/
	
	public Set<String> getKeys(storeType type){
		return getStore(type).getKeys();
	}
	public void deleteSecondary(storeType type,Transaction tx,String dbName){
		getStore(type).deleteSecondary(tx, dbName);
	}
	
	public void createSecondaryIfNeed(storeType type,Transaction tx,String dbName){
		getStore(type).createSecondaryIfNeeded(tx,dbName);
	}
	
	public boolean containsIndex(storeType type,String key){
		return getStore(type).containsIndex(key);
	}
	

}
