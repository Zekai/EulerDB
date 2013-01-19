package org.eulerdb.kernel.storage;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Comparator;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

public class EulerDBHelper {

	// private static EulerDBHelper instance = null;

	private Environment dbEnv = null;

	private DatabaseConfig dbConf = null;

	private boolean mTransactional;

	protected Comparator<byte[]> btreeComparisonFunction = null;

	protected boolean keyPrefixing = true;

	public EulerDBHelper(String path, boolean transactional) {
		mTransactional = transactional;

		if (!mTransactional) {
			if (dbEnv == null) {

				EnvironmentConfig envConf = new EnvironmentConfig();
				// environment will be created if not exists
				envConf.setAllowCreate(true);
				File f = new File(path);
				if (!f.exists())
					f.mkdir();
				dbEnv = new Environment(new File(path), envConf);
			}
			if (dbConf == null) {
				// create a configuration for DB
				dbConf = new DatabaseConfig();
				// db will be created if not exits
				dbConf.setAllowCreate(true);
				dbConf.setDeferredWrite(true);
			}
		} else {

			if (dbEnv == null) {

				File f = new File(path);
				if (!f.exists())
					f.mkdir();

				EnvironmentConfig myEnvConfig = new EnvironmentConfig();
				myEnvConfig.setAllowCreate(true);
				myEnvConfig.setTransactional(true);

				dbEnv = new Environment(f, myEnvConfig);
			}

			// if (txn == null)
			// txn = dbXAEnv.beginTransaction(null, null);

			if (dbConf == null) {
				dbConf = new DatabaseConfig();
				if (btreeComparisonFunction != null) {
					dbConf.setBtreeComparator((Class<? extends Comparator<byte[]>>) btreeComparisonFunction
							.getClass());
				}
				dbConf.setTransactional(true);
				dbConf.setAllowCreate(true);
				dbConf.setSortedDuplicates(true);
				dbConf.setKeyPrefixing(keyPrefixing);
			}
		}

	}

	public Environment getEnvironment() {
			return dbEnv;
	}

	/*
	 * public Transaction getTransaction() { return txn; }
	 */

	public DatabaseConfig getDatabaseConfig() {
		return dbConf;
	}

}
