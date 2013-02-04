package org.eulerdb.kernel.iterator;

import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.tinkerpop.blueprints.Element;

public class EdbPrimaryCursor implements EdbBaseCursor{

	private Cursor mCur;
	private OperationStatus hasNext;

	private long cnt;
	private long max;

	public EdbPrimaryCursor(Cursor cur) {
		this.mCur = cur;

		max = cur.getDatabase().count();
		cnt = 0;

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);

	}

	public void close() {
		logger.debug("closing cursor");
		mCur.close();
	}

	public boolean hasNext() {
		if (cnt >= max) {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();//rewind
			hasNext = mCur.getFirst(key, data, LockMode.DEFAULT);//mCur.close();
			logger.debug("hasNext false");
			return false;
		}
		else
		{
			logger.debug("hasNext true");
			return true;
		}
		
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
		logger.debug("next:" + ((Element)v).getId());
		return v;
	}

	public void remove() {
		logger.debug("remove cursor");
		mCur.delete();
	}

}
