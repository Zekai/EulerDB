package org.eulerdb.kernel.storage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.eulerdb.kernel.helper.FileHelper;

public class EdbManager {
	private static Map<String, EdbStorage> mDbInstances;
	private static String sBaseFoler;

	static {
		mDbInstances = new Hashtable<String, EdbStorage>();
		Properties prop = new Properties();
		try {
			prop.load(EdbManager.class.getResourceAsStream("/config.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sBaseFoler = prop.getProperty("BaseFolder");
	}

	public static EdbStorage requestDbInstance(String dbname, boolean isTransactioanl,
			boolean autoindex){
		
		
		EdbStorage e = mDbInstances.get(dbname);
		Assert.assertNull(e);
		if(e==null) {
			e = new EdbStorage(dbname,isTransactioanl,autoindex);
			mDbInstances.put(dbname, e);
		}
		return e;
	}
	
	public void close(){
		for(EdbStorage e:mDbInstances.values()){
			e.close();
		}
	}

	public static EdbStorage getDbInstance(String key) {
		return mDbInstances.get(key);
	}
	
	public static void closeInstance(String key) {
		EdbStorage e = mDbInstances.get(key);
		if (e != null) {
			e.close();
			mDbInstances.remove(key);
		}
	}

	public static String getBase(){
		return sBaseFoler;
	}
	
	public static void deleteEnv(String dbName){
		if(dbName==null||"".equals(dbName)) throw new IllegalArgumentException("DbName cant be null!");
		closeInstance(dbName);
		FileHelper.deleteDir(FileHelper.appendFileName(sBaseFoler,dbName));
	}
}
