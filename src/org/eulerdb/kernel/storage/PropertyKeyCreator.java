package org.eulerdb.kernel.storage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryMultiKeyCreator;

import com.sleepycat.bind.tuple.TupleBinding;

public class PropertyKeyCreator implements SecondaryKeyCreator {

    private TupleBinding theBinding;
    private String mKey;

    // Use the constructor to set the tuple binding
    PropertyKeyCreator(TupleBinding binding,String key) {
        theBinding = binding;
        mKey = key;
    }

    
    // Abstract method that we must implement
    public boolean createSecondaryKey(SecondaryDatabase secDb,
        DatabaseEntry keyEntry,    // From the primary
        DatabaseEntry dataEntry,   // From the primary
        DatabaseEntry resultEntry) // set the key data on this.
        throws DatabaseException {

            // Convert dataEntry to an Inventory object
        	Map<Object, Object> prperties = 
                (Hashtable<Object, Object>) theBinding.entryToObject(dataEntry);
            // Get the item name and use that as the key
        	Object theItem = prperties.get(mKey);
            try {
				resultEntry.setData(ByteArrayHelper.serialize(theItem));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        
        return true;
    }

    /*
	@Override
	public synchronized void createSecondaryKeys(SecondaryDatabase arg0, DatabaseEntry keyEntry,
			DatabaseEntry dataEntry, Set<DatabaseEntry> arg3) {
		 // Convert dataEntry to an Inventory object
    	Map<String, Object> prperties = 
            (Hashtable<String, Object>) theBinding.entryToObject(dataEntry);
       
    	for(Map.Entry<String,Object> p:prperties.entrySet()){
    		DatabaseEntry r = new DatabaseEntry();
    		 Object theItem = (String) prperties.get(p.getKey());
    		 try {
				r.setData(ByteArrayHelper.serialize(theItem));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		 arg3.add(r);
    	}
    	
    	for(Object p:prperties.values()){
    		DatabaseEntry r = new DatabaseEntry();
    		 try {
				r.setData(ByteArrayHelper.serialize(p));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		 arg3.add(r);
    	}
	}*/
} 