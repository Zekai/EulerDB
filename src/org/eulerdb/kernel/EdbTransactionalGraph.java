package org.eulerdb.kernel;

import javax.transaction.xa.XAException;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * Check this out for 2-phase commit
 * http://en.wikipedia.org/wiki/Two-phase_commit_protocol
 * http://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_txns003.htm
 * 
 * @author Zekai Huang
 * 
 */
public class EdbTransactionalGraph extends EdbGraph implements
		TransactionalGraph {

	private XidImpl xid;
	private XAEnvironment xaEnv;

	static {
		FEATURES.supportsTransactions = true;
	}

	public EdbTransactionalGraph(String path) {
		super(path, true);
		xid = generateXid();
		xaEnv = (XAEnvironment) mEdbHelper.getEnvironment();

	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public void startTransaction() throws IllegalStateException {

		try {
			generateXid();
			mTx = xaEnv.beginTransaction(null, null);
			mEdbHelper.setTransaction(mTx);
			xaEnv.setXATransaction(xid, mTx);
		} catch (IllegalStateException e) {

		}
	}

	@Override
	public void stopTransaction(Conclusion conclusion) {
		try {
			if (Conclusion.SUCCESS == conclusion)
				commit();
			else
				rollback();
		} catch (XAException e) {

		}
	}

	private XidImpl generateXid() {
		String id = java.util.UUID.randomUUID().toString();
		XidImpl xid = new XidImpl(1, id.getBytes(), "TwoPCTest1".getBytes());

		return xid;
	}

	public int prepare() throws XAException {
		return xaEnv.prepare(xid);
	}

	private void commit() throws XAException {
		xaEnv.commit(xid, false);
	}

	private void rollback() throws XAException {
		xaEnv.rollback(xid);
		mCache.clear();// invalidate the previous caching. The caching doesn't
						// really support rollback. when rolling back, it simply
						// rebuild caching
	}

}
