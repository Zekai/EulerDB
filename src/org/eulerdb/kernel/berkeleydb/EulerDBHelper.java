package org.eulerdb.kernel.berkeleydb;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Comparator;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.config.EnvironmentParams;

public class EulerDBHelper {

	// private static EulerDBHelper instance = null;

	private Environment dbEnv = null;

	private XAEnvironment dbXAEnv = null;

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

			if (dbXAEnv == null) {

				File f = new File(path);
				if (!f.exists())
					f.mkdir();
				EnvironmentConfig envConf = new EnvironmentConfig();
				envConf.setTransactional(true);
				envConf.setAllowCreate(true);
				// envConf.setTxnNoSync(true);
				envConf.setConfigParam(
						EnvironmentParams.ENV_CHECK_LEAKS.getName(), "false");
				envConf.setConfigParam(EnvironmentParams.NODE_MAX.getName(),
						"6");
				envConf.setConfigParam(
						EnvironmentParams.ENV_RUN_CLEANER.getName(), "false");
				envConf.setConfigParam(
						EnvironmentParams.ENV_RUN_EVICTOR.getName(), "false");
				String val = System.getProperty("isolationLevel");
				if (val != null && val.length() > 0) {
					if ("serializable".equals(val)) {
						envConf.setTxnSerializableIsolation(true);
					} else if ("readCommitted".equals(val)) {
						DbInternal.setTxnReadCommitted(envConf, true);
					} else {
						throw new IllegalArgumentException(
								"Unknown isolationLevel system property value: "
										+ val);
					}
				}

				dbXAEnv = new XAEnvironment(new File(path), envConf);
			}

//			if (txn == null)
//				txn = dbXAEnv.beginTransaction(null, null);

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
		if (mTransactional)
			return dbXAEnv;
		else
			return dbEnv;
	}

	/*
	public Transaction getTransaction() {
		return txn;
	}*/

	public DatabaseConfig getDatabaseConfig() {
		return dbConf;
	}

}
