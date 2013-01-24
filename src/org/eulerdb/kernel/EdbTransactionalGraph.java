package org.eulerdb.kernel;

import javax.transaction.xa.XAException;

import org.eulerdb.kernel.storage.EulerDBHelper;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.Transaction.State;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Check this out for 2-phase commit
 * http://en.wikipedia.org/wiki/Two-phase_commit_protocol
 * http://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_txns003.htm
 * 
 * "Transactions are bound to the current thread, which means that any graph
 * operation executed by the thread occurs in the context of that transaction
 * and that there may only be one thread executing in a single transaction."
 * 
 * "A transactional graph supports the notion of transactions. Once a transaction
 * is started, all write operations can either be committed or rolled back. Read
 * operations are not required to be in a transaction. A transactional graph can
 * be in two modes: automatic or manual. All constructed transactional graphs
 * begin in automatic transaction mode."
 * 
 * @author Zekai Huang
 * 
 */
public class EdbTransactionalGraph extends EdbGraph implements
		TransactionalGraph {

	private enum Status {
		FRESH, NEW, END
	};

	private Status mStatus;

	static {
		FEATURES.supportsTransactions = true;
	}

	public EdbTransactionalGraph(String path) {
		super(path, true);
		autoStartTransaction();
		mStatus = Status.FRESH;
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
	}

	private void autoStartTransaction(){
		if(mTx == null) mTx = mEdbHelper.getEnvironment().beginTransaction(null, null);
	}

	@Override
	public Edge addEdge(Object id, Vertex n1, Vertex n2, String relation) {
		autoStartTransaction();
		return super.addEdge(id, n1, n2, relation);
	}

	@Override
	public Vertex addVertex(Object id) {
		autoStartTransaction();
		return super.addVertex(id);
	}

	@Override
	public Edge getEdge(Object arg0) {
		autoStartTransaction();
		return super.getEdge(arg0);
	}

	@Override
	public Iterable<Edge> getEdges() {
		autoStartTransaction();
		return super.getEdges();
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		autoStartTransaction();
		return super.getEdges(arg0, arg1);
	}

	@Override
	public Vertex getVertex(Object arg0) {
		autoStartTransaction();
		return super.getVertex(arg0);
	}

	@Override
	public Iterable<Vertex> getVertices() {
		autoStartTransaction();
		return super.getVertices();
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		autoStartTransaction();
		return super.getVertices(arg0, arg1);
	}

	@Override
	public void removeEdge(Edge arg0) {
		autoStartTransaction();
		super.removeEdge(arg0);
		
	}

	@Override
	public void removeVertex(Vertex arg0) {
		autoStartTransaction();
		super.removeVertex(arg0);
		
	}
	
	@Override
	public void startTransaction() throws IllegalStateException {

		if (mStatus == Status.NEW)
			throw new IllegalStateException(
					"previous transaction has not ended.");
		else if (mStatus == Status.END) {
			try {
				autoStartTransaction();
				mStatus = Status.NEW;
			} catch (IllegalStateException e) {

			}
		} else {
			mStatus = Status.NEW;
		}
	}

	@Override
	public void stopTransaction(Conclusion conclusion) {
		mStorage.closeCursor();
		try {
			if (Conclusion.SUCCESS == conclusion)
				commit();
			else
				abort();
		} catch (XAException e) {

		}

	}

	private void commit() throws XAException {
		if(mTx==null) return;
		mTx.commit();
		mTx = null;
		mStatus = Status.END;
	}

	private void abort() throws XAException {
		if(mTx==null) return;
		mTx.abort();
		mTx = null;
		mStorage.resetCache(mTx);
		mStatus = Status.END;
	}

	@Override
	public void shutdown() {
		mStorage.closeCursor();
		if (mTx != null) {
			try {
				commit();
			} catch (XAException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		super.shutdown();
	}

}
