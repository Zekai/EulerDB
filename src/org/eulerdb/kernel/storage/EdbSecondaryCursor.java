package org.eulerdb.kernel.storage;

import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

public class EdbSecondaryCursor {

	private SecondaryCursor mCur;
	private OperationStatus hasNext;
	private DatabaseEntry searchKey;
	private DatabaseEntry primaryKey =null;
	private DatabaseEntry primaryData = null; 

	private long cnt;
	private long max;

	public EdbSecondaryCursor(final SecondaryCursor cur, String secondaryKey) {
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
			//mCur.close();
			return false;
		}
		else{
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
		
		return vid;
	}

	public void remove() {
		mCur.delete();
	}
	
    @Override
	public void finalize(){
		mCur.close();
	}

}
