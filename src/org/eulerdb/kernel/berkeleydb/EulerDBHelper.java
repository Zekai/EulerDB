package org.eulerdb.kernel.berkeleydb;

import java.io.File;
import java.io.FilenameFilter;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class EulerDBHelper {

	private static EulerDBHelper instance = null;

	private static Environment dbEnv = null;

	private static DatabaseConfig dbConf = null;

	public static EulerDBHelper getInstance() {
		if (instance == null) {
			instance = new EulerDBHelper();
		}
		return instance;
	}

	public static void init(String path) {
		if (dbEnv == null) {
			EnvironmentConfig envConf = new EnvironmentConfig();
			// environment will be created if not exists
			envConf.setAllowCreate(true);
			File f = new File(path);
			if (!f.exists())
				f.mkdir();
			dbEnv = new Environment(new File(path), envConf);

			// create a configuration for DB
			dbConf = new DatabaseConfig();
			// db will be created if not exits
			dbConf.setAllowCreate(true);
			dbConf.setDeferredWrite(true);
		}

	}

	public static Environment getEnvironment() {
		return dbEnv;
	}

	public static DatabaseConfig getDatabaseConfig() {
		return dbConf;
	}

	public static FilenameFilter mDatabaseFilter = new FilenameFilter() {
		public boolean accept(File dir, String name) {
			return !name.startsWith(".") && name.endsWith(".jdb");
		}
	};

	public static FilenameFilter getDatabaseFilter() {
		return mDatabaseFilter;
	}

}
