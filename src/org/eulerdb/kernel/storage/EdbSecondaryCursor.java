package org.eulerdb.kernel.storage;

import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.tinkerpop.blueprints.Element;

public class EdbSecondaryCursor implements EdbBaseCursor{

	private SecondaryCursor mCur;
	private OperationStatus hasNext;
	private DatabaseEntry searchKey;
	private DatabaseEntry primaryKey =null;
	private DatabaseEntry primaryData = null; 

	private long cnt;
	private long max;

	public EdbSecondaryCursor(final SecondaryCursor cur, Object secondaryKey) {
		this.mCur = cur;

		try {
			searchKey = new DatabaseEntry(ByteArrayHelper.serialize(secondaryKey));
			primaryKey = new DatabaseEntry();
			primaryData = new DatabaseEntry();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//mCur = (SecondaryCursor) mCur.getDatabase().openCursor(null,null);
		hasNext = mCur.getSearchKey(searchKey,primaryKey, primaryData, LockMode.DEFAULT);
		
		if(hasNext==OperationStatus.SUCCESS)
		{
			max = mCur.count();
			cnt = 0;
		}
		else
		{
			max = 0;
			cnt = 0;
		}


	}

	public void close() {
		mCur.close();
	}
	
	public Object getFirst() {
		cnt = 0;
		Object v = null;
		primaryKey = new DatabaseEntry();
		primaryData = new DatabaseEntry();
		hasNext = mCur.getSearchKey(searchKey,primaryKey, primaryData, LockMode.DEFAULT);
		try {
			v = ByteArrayHelper.deserialize(primaryKey.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}

	public boolean hasNext() {
		if (cnt >= max) {
			logger.debug("hasNext false");
			return false;
		}
		else{
			logger.debug("hasNext true");
			return true;
		}

		/*
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		hasNext = mCur.getCurrent(key, data, LockMode.DEFAULT);

		boolean result = (hasNext == OperationStatus.SUCCESS) && (key.getData() != null);
		
		if(!result) 
			mCur.close();
			*/
	}


	public Object next() {
		
		if (cnt == 0) {
		} else {
			mCur.getNextDup(searchKey,primaryKey, primaryData, LockMode.DEFAULT);
		}
		cnt++;
		String vid = null;
		try {
			vid = (String) ByteArrayHelper.deserialize(primaryKey.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.debug("next:" + vid);
		return vid;
	}

	public void remove() {
		mCur.delete();
	}
}
