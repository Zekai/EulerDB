package org.eulerdb.kernel;

import javax.transaction.xa.XAException;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * Check this out for 2-phase commit
 * http://en.wikipedia.org/wiki/Two-phase_commit_protocol
 * http://docs.oracle.com/cd/B28359_01/server.111/b28310/ds_txns003.htm
 * 
 * "Transactions are bound to the current thread, which means that any graph
 * operation executed by the thread occurs in the context of that transaction
 * and that there may only be one thread executing in a single transaction."
 * 
 * "A transactional graph supports the notion of transactions. Once a
 * transaction is started, all write operations can either be committed or
 * rolled back. Read operations are not required to be in a transaction. A
 * transactional graph can be in two modes: automatic or manual. All constructed
 * transactional graphs begin in automatic transaction mode."
 * 
 * @author Zekai Huang
 * 
 */
public class EdbTransactionalGraph extends EdbKeyIndexableGraph implements
		TransactionalGraph {

	public final static ThreadLocal<Transaction> txs = new ThreadLocal<Transaction>() {
		protected Transaction initialValue() {
			return null;
		}
	};

	static {
		FEATURES.supportsTransactions = true;
	}

	public EdbTransactionalGraph(String path) {
		super(path, true,false);
	}
	
	public EdbTransactionalGraph(String path,boolean isTransactional,boolean autoIndex) {
		super(path, isTransactional,autoIndex);
	}

	@Override
	public Features getFeatures() {
		return FEATURES;
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
		//autoStartTransaction();
		return super.getEdge(arg0);
	}

	@Override
	public Iterable<Edge> getEdges() {
		//autoStartTransaction();
		return super.getEdges();
	}

	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		//autoStartTransaction();
		return super.getEdges(arg0, arg1);
	}

	@Override
	public Vertex getVertex(Object arg0) {
		//autoStartTransaction();
		return super.getVertex(arg0);
	}

	@Override
	public Iterable<Vertex> getVertices() {
		//autoStartTransaction();
		return super.getVertices();
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		//autoStartTransaction();
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

	private void autoStartTransaction() {
		if (txs.get() == null) {
			txs.set(mEdbHelper.getEnvironment().beginTransaction(null, null));
		}
	}

	@Override
	public void startTransaction() throws IllegalStateException {
		if (txs.get() == null) {
			txs.set(mEdbHelper.getEnvironment().beginTransaction(null, null));
		} else
			throw ExceptionFactory.transactionAlreadyStarted();

	}

	@Override
	public void stopTransaction(Conclusion conclusion) {
		mStorage.closeCursor();
		if (null == txs.get()) {
			return;
		}

		try {
			if (conclusion.equals(Conclusion.SUCCESS))
				commit();
			else
				abort();
		} catch (XAException e)
		{
			
		}finally {
			txs.remove();
		}

	}
	
	@Override
	protected Transaction getTransaction(){
		return txs.get();
	}

	private void commit() throws XAException {
		txs.get().commit();
	}

	private void abort() throws XAException {
		txs.get().abort();
	}

	@Override
	public void shutdown() {
		if(mIsRunning){
			mStorage.closeCursor();
			if (null != txs.get()) {
				txs.get().commit();
				txs.remove();
			}
			super.shutdown();
		}
	}

}
