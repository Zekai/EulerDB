package org.eulerdb.kernel.storage;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.helper.ByteArrayHelper;
import org.eulerdb.kernel.helper.EdbHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class EdbKeyPairStore {
	
	protected Logger logger = Logger.getLogger(this.getClass().getCanonicalName());

	private Database mStore;
	private String mPrimaryName;
	private Map<String,SecondaryDatabase> secondDBs;
	private Map<String,SecondaryCursor> secondCursors;
	private boolean mAutoIndex;
	private Environment mEnv;

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

	public EdbKeyPairStore(Environment env,DatabaseConfig conf, String name,
			boolean secondary) {
		mEnv = env;
		mAutoIndex = secondary;
		// Transaction txn0 = edbHelper.getEnvironment().beginTransaction(null,
		// null);
		mStore =mEnv.openDatabase(null, name,
				conf);
		mPrimaryName = name;
		logger.debug("primary database name of "+ mPrimaryName);
		secondDBs = new Hashtable<String, SecondaryDatabase>();
		secondCursors = new Hashtable<String, SecondaryCursor>();
		// txn0.commit();
		//if(name.equals(Common.VERTEXPROPERTY)) createSecondaryIfNeed("name");
		loadSecondary(null);
	};

	public OperationStatus put(Transaction tx, byte[] key, byte[] value) {
		logger.debug("put record to database ");
		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_value = new DatabaseEntry(value);
		// mStore.delete(tx, d_key);
		OperationStatus r =  mStore.put(tx, d_key, d_value);
		
		logger.debug("put record to database "+ r);
		return r;
	}

	public OperationStatus put(Transaction tx, DatabaseEntry d_key,
			DatabaseEntry d_value) {

		OperationStatus r = mStore.put(tx, d_key, d_value);
		logger.debug("put record to database "+ r);
		return r;
	}

	public DatabaseEntry get(Transaction tx, DatabaseEntry id) {
		logger.debug("get record from database ");
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(tx, id, data, LockMode.DEFAULT);
		return data;
	}

	public byte[] get(Transaction tx, byte[] id) {
		logger.debug("get record from database ");
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(tx, new DatabaseEntry(id), data, LockMode.DEFAULT);
		return data.getData();
	}

	public OperationStatus delete(Transaction tx, byte[] id) {
		logger.debug("delete record from database ");
		DatabaseEntry key = new DatabaseEntry(id);
		return mStore.delete(tx, key);

	}

	public long count() {
		
		long r=  mStore.count();
		logger.debug("get count from database "+ r);
		return r;
	}

	public void close() {
		logger.debug("closing KeyPair store ");
		for(SecondaryCursor scondCursor:secondCursors.values()){
			logger.debug("closing SecondaryCursor ");
			secondCursors.remove(scondCursor);
			scondCursor.close();
		}
		for (SecondaryDatabase secondDB : secondDBs.values()) {
			logger.debug("closing secondDB ");
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
		logger.debug("closing KeyPair store ");
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
	
	public boolean containsIndex(String key){
		boolean r= secondDBs.containsKey(key);
		logger.debug("autoindex of " + key +" exists "+ r);
		return r;
	}

	public synchronized void createSecondaryIfNeeded(Transaction tx,String key) {
		
		if (!mAutoIndex||secondDBs.containsKey(key))
			return;
		
		SecondaryConfig mySecConfig = new SecondaryConfig();

		// If the environment is read-only, then
		// make the databases read-only too.
		 mySecConfig.setReadOnly(false);
		 mySecConfig.setTransactional(mStore.getConfig().getTransactional());
		// If the environment is opened for write, then we want to be
		// able to create the environment and databases if
		// they do not exist.
		mySecConfig.setAllowCreate(true);

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
		String name = mPrimaryName+Common.SEPARATOR_PRIME2ND+key;
		logger.debug("create SecondaryDatabase:"+ name);
		SecondaryDatabase secondDb = mEnv
				.openSecondaryDatabase(tx, name, // Index name
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
		logger.debug("remove SecondaryDatabase:"+ dbName);
		secondDBs.remove(dbName);
		mEnv.removeDatabase(tx, dbName);
	}

	public SecondaryCursor getSecondCursor(String dbName, Transaction tx) {
		/*if(!mAutoIndex)
			throw new UnsupportedOperationException("auto index is set to false, no secondary cursor is allowed.");
		String key = dbName+"_"+EdbHelper.getTransactionId(tx);
		
		if(secondCursors.containsKey(key)) return secondCursors.get(key);*/
		
		//System.out.println(key);
		logger.debug("getSecondCursor:"+ dbName);
		SecondaryDatabase secondDb = secondDBs.get(dbName);
		SecondaryCursor mySecCursor = secondDb.openCursor(tx, null);// openSecondaryCursor(null,
		secondCursors.put(dbName, mySecCursor);
		return mySecCursor;
	}
	
	public void loadSecondary(Transaction tx){
		List<String> dbNames = mEnv.getDatabaseNames();
		String prefix = mPrimaryName+Common.SEPARATOR_PRIME2ND;
		logger.debug("loadSecondary prefix:"+ prefix);
		for(String dbName:dbNames){
			if(dbName.startsWith(prefix))
			{
				String secondaryDbName = dbName.substring(prefix.length());
				logger.debug("loadSecondary secondaryDbName:"+ secondaryDbName);
				createSecondaryIfNeeded(tx,secondaryDbName);
			}
		}
	}

}
