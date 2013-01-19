package org.eulerdb.kernel.storage;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.eulerdb.kernel.EdbVertex;

import com.sleepycat.je.Transaction;

public class EdbCaching {
	
	/**
	 * TODO: use transaction as region name, use object id as the cache key
	 */

	private static final Logger logger = Logger.getLogger(EdbCaching.class
			.getCanonicalName());

	private static JCS cache = null;

	private static final String cacheRegionName = "vertex";
	private static final String regionKey = cacheRegionName + "Id:";

	private static EdbCaching instance = null;

	public EdbCaching() {

		// Set up a simple configuration that logs on the console.
		BasicConfigurator.configure();

		CompositeCacheManager ccm = CompositeCacheManager
				.getUnconfiguredInstance();
		Properties props = new Properties();
		Reader r;
		try {
			r = new FileReader("cache.ccf");
			props.load(r);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ccm.configure(props);

		// initialize the cache
		try {
			cache = JCS.getInstance(cacheRegionName);
		} catch (CacheException e) {
			logger.error("Problem initializing cache for region name ["
					+ cacheRegionName + "].", e);
		}
	}

	public static EdbCaching getInstance() {
		if (instance == null) {
			instance = new EdbCaching();
		}
		return instance;
	}

	public void put(String id, Long tid,EdbVertex n) {
		String key = regionKey + id+"_"+tid;
		try {
			// if it isn't null, insert it
			if (n != null) {
				cache.put(key, n);
			}
		} catch (CacheException e) {
			logger.error("Problem putting " + n.getId()
					+ " in the cache, for key " + n.getId(), e);
		}
	}

	public EdbVertex get(String id,Long tid) {
		String key = regionKey + id+"_"+tid;
		return (EdbVertex) cache.get(key);
	}

	public void remove(String id,Long tid) {
		String key = regionKey + id+"_"+tid;
		try {
			cache.remove(key);
		} catch (CacheException e) {
			// TODO Auto-generated catch block
			logger.error("Problem removing " + id + " in the cache", e);
		}
	}
	
	public void clear(Long id) {
		try {
			cache.clear();
		} catch (CacheException e) {
			// TODO Auto-generated catch block
			logger.error("Problem clearing the cache", e);
		}
	}
	

}
