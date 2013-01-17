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

	public OperationStatus put(byte[] key, byte[] value) {
		
		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_value = new DatabaseEntry(value);
		mStore.delete(mEdbHelper.getTransaction(), d_key);
		return mStore.put(mEdbHelper.getTransaction(), d_key, d_value);
	}

	public byte[] get(byte[] id) {
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(mEdbHelper.getTransaction(), new DatabaseEntry(id), data, LockMode.DEFAULT);
		return data.getData();
	}

	public OperationStatus delete(byte[] id) {
		DatabaseEntry key = new DatabaseEntry(id);
		return mStore.delete(mEdbHelper.getTransaction(), key);
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

	public Cursor getCursor() {
		return mStore.openCursor(mEdbHelper.getTransaction(), null);
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
