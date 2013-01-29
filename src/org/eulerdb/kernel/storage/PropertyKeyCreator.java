package org.eulerdb.kernel.storage;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

import com.sleepycat.bind.tuple.TupleBinding;

public class PropertyKeyCreator implements SecondaryKeyCreator {

    private TupleBinding theBinding;

    // Use the constructor to set the tuple binding
    PropertyKeyCreator(TupleBinding binding) {
        theBinding = binding;
    }

    // Abstract method that we must implement
    public boolean createSecondaryKey(SecondaryDatabase secDb,
        DatabaseEntry keyEntry,    // From the primary
        DatabaseEntry dataEntry,   // From the primary
        DatabaseEntry resultEntry) // set the key data on this.
        throws DatabaseException {

            // Convert dataEntry to an Inventory object
        	Map<String, Object> prperties = 
                (Hashtable<String, Object>) theBinding.entryToObject(dataEntry);
            // Get the item name and use that as the key
            String theItem = (String) prperties.get("name");
            try {
				resultEntry.setData(theItem.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        
        return true;
    }
} 