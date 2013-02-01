package org.eulerdb.kernel.storage;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class EdbKeyPairStore {

	private Database mStore;
	private EulerDBHelper mEdbHelper = null;
	private Map<String,SecondaryDatabase> secondDBs;
	private Map<String,SecondaryCursor> secondCursors;
	private boolean mAutoIndex;

	// private static EdbKeyPairStore instance = null;

	/*
	 * public static EdbKeyPairStore getInstance(String path) { if (instance ==
	 * null) { EulerDBHelper.init(path); instance = new EdbKeyPairStore(path); }
	 * return instance; }
	 * 
	 * public static EdbKeyPairStore getInstance(Object c) {
	 * if(c.getClass().isInstance(EdbGraph.class)) { throw new
	 * IllegalArgumentException(
	 * "EdbGraph.class needs to pass in the path, use getInstance(String path) instead"
	 * ); } assert(instance!=null); return instance; }
	 */

	public EdbKeyPairStore(EulerDBHelper edbHelper, String name,
			boolean secondary) {
		mEdbHelper = edbHelper;
		mAutoIndex = secondary;
		// Transaction txn0 = edbHelper.getEnvironment().beginTransaction(null,
		// null);
		mStore = mEdbHelper.getEnvironment().openDatabase(null, name,
				mEdbHelper.getDatabaseConfig());

		secondDBs = new Hashtable<String, SecondaryDatabase>();
		secondCursors = new Hashtable<String, SecondaryCursor>();
		// txn0.commit();
		//if(name.equals(Common.VERTEXPROPERTY)) createSecondaryIfNeed("name");
	};

	public OperationStatus put(Transaction tx, byte[] key, byte[] value) {

		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_value = new DatabaseEntry(value);
		// mStore.delete(tx, d_key);
		return mStore.put(tx, d_key, d_value);
	}

	public OperationStatus put(Transaction tx, DatabaseEntry d_key,
			DatabaseEntry d_value) {

		return mStore.put(tx, d_key, d_value);
	}

	public DatabaseEntry get(Transaction tx, DatabaseEntry id) {
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(tx, id, data, LockMode.DEFAULT);
		return data;
	}

	public byte[] get(Transaction tx, byte[] id) {
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(tx, new DatabaseEntry(id), data, LockMode.DEFAULT);
		return data.getData();
	}

	public OperationStatus delete(Transaction tx, byte[] id) {
		DatabaseEntry key = new DatabaseEntry(id);
		return mStore.delete(tx, key);

	}

	public long count() {
		return mStore.count();
	}

	public void close() {
		for(SecondaryCursor scondCursor:secondCursors.values()){
			secondCursors.remove(scondCursor);
			scondCursor.close();
		}
		for (SecondaryDatabase secondDB : secondDBs.values()) {
			secondDB.close();
		}
		
		secondDBs = null;
		mStore.close();
		mStore = null;

	}

	public void sync() {
		mStore.sync();
	}

	public Cursor getCursor(Transaction tx) {
		CursorConfig curconf = new CursorConfig();
		curconf.setReadUncommitted(true);
		return mStore.openCursor(tx, curconf);
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

	public void createSecondary(boolean readOnly, String key) {

		if (!mAutoIndex||secondDBs.containsKey(key))
			return;

		SecondaryConfig mySecConfig = new SecondaryConfig();

		// If the environment is read-only, then
		// make the databases read-only too.
		 mySecConfig.setReadOnly(readOnly);

		// If the environment is opened for write, then we want to be
		// able to create the environment and databases if
		// they do not exist.
		mySecConfig.setAllowCreate(!readOnly);

		// Environment and database opens omitted for brevity

		// Open the secondary database. We use this to create a
		// secondary index for the inventory database

		// We want to maintain an index for the inventory entries based
		// on the item name. So, instantiate the appropriate key creator
		// and open a secondary database.
		PropertyKeyCreator keyCreator = new PropertyKeyCreator(
				new PropertyBinding(), key);

		// Set up the secondary properties
		mySecConfig.setAllowPopulate(true); // Allow autopopulate
		mySecConfig.setKeyCreator(keyCreator);
		// Need to allow duplicates for our secondary database
		mySecConfig.setSortedDuplicates(true);

		// Now open it
		String primarydbname = mStore.getDatabaseName();
		SecondaryDatabase secondDb = mEdbHelper.getEnvironment()
				.openSecondaryDatabase(null, primarydbname+Common.SEPARATOR_PRIME2ND+key, // Index name
						mStore, // Primary database handle. This is
								// the db that we're indexing.
						mySecConfig); // The secondary config

		secondDBs.put(key, secondDb);
	}

	/*public void openSecondary(String key) {
		createSecondary(true, key);
	}*/

	public Set<String> getKeys() {
		return secondDBs.keySet();
	}

	public void deleteSecondary(Transaction tx, String dbName) {
		secondDBs.remove(dbName);
		mEdbHelper.getEnvironment().removeDatabase(tx, dbName);
	}

	public SecondaryCursor getSecondCursor(String dbName, Transaction tx) {
		if(!mAutoIndex)
			throw new UnsupportedOperationException("auto index is set to false, no secondary cursor is allowed.");
		String key = dbName+"_"+EdbHelper.getTransactionId(tx);
		
		if(secondCursors.containsKey(key)) return secondCursors.get(key);
		System.out.println(key);
		SecondaryDatabase secondDb = secondDBs.get(dbName);

		SecondaryCursor mySecCursor = secondDb.openCursor(tx, null);// openSecondaryCursor(null,
		secondCursors.put(key, mySecCursor);
		return mySecCursor;
	}

	public void createSecondaryIfNeed(String searchName) {
		createSecondary(false, searchName);
	}

}
