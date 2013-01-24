package org.eulerdb.kernel.storage;


import java.io.IOException;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;


/**
 * @author Mark Hayes
 */
public class TestEntityBinding implements EntityBinding {

    private boolean isRecNum;

    public TestEntityBinding(boolean isRecNum) {

        this.isRecNum = isRecNum;
    }

	@Override
	public Object entryToObject(DatabaseEntry arg0, DatabaseEntry arg1) {
		// TODO Auto-generated method stub
		try {
			return ByteArrayHelper.deserialize(arg1.getData());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void objectToData(Object arg0, DatabaseEntry arg1) {
		try {
			arg1 = new DatabaseEntry(ByteArrayHelper.serialize(arg0));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void objectToKey(Object arg0, DatabaseEntry arg1) {
		try {
			arg1 = new DatabaseEntry(ByteArrayHelper.serialize(arg0));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

    
}