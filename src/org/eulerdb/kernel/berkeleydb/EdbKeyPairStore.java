package org.eulerdb.kernel.berkeleydb;

import java.io.File;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class EdbKeyPairStore {

	public Database mStore;
	private Transaction txn = null;

	public EdbKeyPairStore() {
	};

	public EdbKeyPairStore(String name) {

		mStore = EulerDBHelper.getEnvironment().openDatabase(null, name,
				EulerDBHelper.getDatabaseConfig());
	};

	public OperationStatus put(byte[] key, byte[] value) {
		DatabaseEntry d_key = new DatabaseEntry(key);
		DatabaseEntry d_value = new DatabaseEntry(value);
		return mStore.put(txn, d_key, d_value);
	}

	public byte[] get(byte[] id) {
		DatabaseEntry data = new DatabaseEntry();
		mStore.get(txn, new DatabaseEntry(id), data, LockMode.DEFAULT);
		return data.getData();
	}

	public OperationStatus delete(byte[] id) {
		DatabaseEntry key = new DatabaseEntry(id);
		return mStore.delete(txn, key);
	}

	public long count() {
		return mStore.count();
	}

	public void close() {
		mStore.close();
	}

	public void sync() {
		mStore.sync();

	}

	public Cursor getCursor() {
		return mStore.openCursor(txn, null);
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

}
