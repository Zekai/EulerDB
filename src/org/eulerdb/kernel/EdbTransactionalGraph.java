package org.eulerdb.kernel;

import javax.transaction.xa.XAException;

import org.eulerdb.kernel.storage.EulerDBHelper;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * Check this out for 2-phase commit
 * http://en.wikipedia.org/wiki/Two-phase_commit_protocol
 * http://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_txns003.htm
 * 
 * "Transactions are bound to the current thread, which means that any graph
 * operation executed by the thread occurs in the context of that transaction
 * and that there may only be one thread executing in a single transaction."
 * 
 * @author Zekai Huang
 * 
 */
public class EdbTransactionalGraph extends EdbGraph implements
		TransactionalGraph {

	static {
		FEATURES.supportsTransactions = true;
	}

	public EdbTransactionalGraph(String path) {
		super(path, true);
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public void startTransaction() throws IllegalStateException {

		try {
			mTx = mEdbHelper.getEnvironment().beginTransaction(null, null);
		} catch (IllegalStateException e) {

		}
	}

	@Override
	public void stopTransaction(Conclusion conclusion) {
		try {
			if (Conclusion.SUCCESS == conclusion)
				commit();
			else
				abort();
		} catch (XAException e) {

		}
	}

	private void commit() throws XAException {
		mTx.commit();
	}

	private void abort() throws XAException {
		mTx.abort();
		mStorage.resetCache(mTx);
	}

}
