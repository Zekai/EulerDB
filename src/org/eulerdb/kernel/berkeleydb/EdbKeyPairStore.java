package org.eulerdb.kernel.berkeleydb;

import junit.framework.Assert;

import org.eulerdb.kernel.helper.ByteArrayHelper;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class EdbKeyPairStore {

	private Database mStore;
	private EulerDBHelper mEdbHelper = null;
	
	
	//private static EdbKeyPairStore instance = null;
	
	/*
	public static EdbKeyPairStore getInstance(String path) {
		if (instance == null) {
			EulerDBHelper.init(path);
			instance = new EdbKeyPairStore(path);
		}
		return instance;
	}

	public static EdbKeyPairStore getInstance(Object c) {
		if(c.getClass().isInstance(EdbGraph.class))
		{
			throw new IllegalArgumentException("EdbGraph.class needs to pass in the path, use getInstance(String path) instead");
		}
		assert(instance!=null);
		return instance;
	}*/
	
	
	
	public EdbKeyPairStore(EulerDBHelper edbHelper,String name) {
		mEdbHelper = edbHelper;
		//Transaction txn0 =  edbHelper.getEnvironment().beginTransaction(null, null);
		mStore = mEdbHelper.getEnvironment().openDatabase(null, name,
				mEdbHelper.getDatabaseConfig());
		//txn0.commit();
	};

	public OperationStatus put(Transaction tx,byte[] key, byte[] value) {
		
		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_value = new DatabaseEntry(value);
		mStore.delete(tx, d_key);
		return mStore.put(tx, d_key, d_value);
	}

	public byte[] get(Transaction tx,byte[] id) {
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(tx, new DatabaseEntry(id), data, LockMode.DEFAULT);
		return data.getData();
	}

	public OperationStatus delete(Transaction tx,byte[] id) {
		DatabaseEntry key = new DatabaseEntry(id);
		return mStore.delete(tx, key);
	}

	public long count() {
		return mStore.count();
	}

	public void close() {
		mStore.close();
	}

	public void sync() {
		mStore.sync();

	}

	public Cursor getCursor(Transaction tx) {
		return mStore.openCursor(tx, null);
	}

	public void append(byte[] key, byte[] value) {
		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_data = new DatabaseEntry();
		mStore.get(null, d_key, d_data, LockMode.DEFAULT);
		if (d_data.getData() == null)
			mStore.put(null, d_key, new DatabaseEntry(value));
		else
			mStore.put(
					null,
					d_key,
					new DatabaseEntry(ByteArrayHelper.concatByteArrays(
							d_data.getData(), value)));
	}

}
