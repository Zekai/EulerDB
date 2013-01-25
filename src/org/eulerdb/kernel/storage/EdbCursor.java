package org.eulerdb.kernel.storage;

import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbHelper;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Vertex;

import com.tinkerpop.blueprints.Element;

public class EdbCursor {

	private Cursor mCur;
	private storeType mType;
	private OperationStatus hasNext;
	private static EdbStorage mStorage;
	private long cnt;
	private long max;
	private Transaction mTx;

	public EdbCursor(storeType type,Cursor cur,Transaction tx) {
		mType = type;
		this.mCur = cur;
		//mCache = EdbCaching.getInstance();
		max = cur.getDatabase().count();
		cnt = 0;
		mTx = tx;
		mStorage = EdbStorage.getInstance();
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);

	}

	public void close() {
		mCur.close();
	}

	public boolean hasNext() {
		if (cnt >= max) {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();//rewind
			hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);//mCur.close();
			return false;
		}

		/*
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getCurrent(key, data, LockMode.DEFAULT);

		boolean result = (hasNext == OperationStatus.SUCCESS) && (key.getData() != null);
		
		if(!result) 
			mCur.close();
			*/
		
		return true;
	}

	public Object getFirst() {
		cnt = 0;
		Object v = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);
		try {
			v = ByteArrayHelper.deserialize(data.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}

	public Object current() {

		Object v = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		mCur.getCurrent(key, data, LockMode.DEFAULT);
		try {
			v = ByteArrayHelper.deserialize(data.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}

	public Object next() {

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		String v = null;

		if (cnt == 0) {
			mCur.getFirst(key, data, LockMode.DEFAULT);
		} else {
			mCur.getNext(key, data, LockMode.DEFAULT);
		}
		cnt++;
		try {
			v = (String) ByteArrayHelper.deserialize(key.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object u = mStorage.getObj(mType, mTx, v);
		return u;
	}

	public void remove() {
		mCur.delete();
	}

}
