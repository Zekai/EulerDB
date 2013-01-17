package org.eulerdb.kernel;

import javax.transaction.xa.XAException;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.XAEnvironment;
import com.sleepycat.je.log.LogUtils.XidImpl;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;


/**
 * Check this out for 2-phase commit http://en.wikipedia.org/wiki/Two-phase_commit_protocol
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
		xid = new XidImpl(1, "TwoPCTest1".getBytes(), null);
		xaEnv = (XAEnvironment) mEdbHelper.getEnvironment();
		xaEnv.setXATransaction(xid, mEdbHelper.getTransaction());
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public void startTransaction() throws IllegalStateException {
		
		try {
			xaEnv.beginTransaction(mEdbHelper.getTransaction(), null);
		} catch (IllegalStateException e) {
			try {
				rollback();
			} catch (XAException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
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

	public int prepare() throws XAException {
		return xaEnv.prepare(xid);
	}

	public void commit() throws XAException {
		xaEnv.commit(xid, false);
	}

	public void rollback() throws XAException {
		xaEnv.rollback(xid);
	}

}
