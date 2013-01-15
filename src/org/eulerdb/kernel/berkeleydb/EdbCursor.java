package org.eulerdb.kernel.berkeleydb;

import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class EdbCursor {

	private Cursor mCur;

	private OperationStatus hasNext;

	private long cnt;
	private long max;

	public EdbCursor(Cursor cur) {
		this.mCur = cur;

		max = cur.getDatabase().count();
		cnt = 0;

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);

	}

	public void close() {
		mCur.close();
	}

	public boolean hasNext() {
		if (cnt >= max) {
			mCur.close();
			return false;
		}

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getCurrent(key, data, LockMode.DEFAULT);

		return (hasNext == OperationStatus.SUCCESS) && (key.getData() != null);
	}

	public Object getFirst() {
		Object v = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		mCur.getFirst(key, data, LockMode.DEFAULT);
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
		Object v = null;

		if (cnt == 0) {
			mCur.getFirst(key, data, LockMode.DEFAULT);
		} else {
			mCur.getNext(key, data, LockMode.DEFAULT);
		}
		cnt++;
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

	public void remove() {
		mCur.delete();
	}

}
