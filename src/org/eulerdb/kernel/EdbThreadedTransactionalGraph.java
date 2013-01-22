package org.eulerdb.kernel;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;

public class EdbThreadedTransactionalGraph extends EdbTransactionalGraph implements  ThreadedTransactionalGraph{
	
	private static final ThreadLocal<Transaction> txs = new ThreadLocal<Transaction>(){
	    protected Transaction initialValue() {
	      return mEdbHelper.getEnvironment().beginTransaction(null, null);
	   }
	  };

	public EdbThreadedTransactionalGraph(String path) {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	public TransactionalGraph startThreadTransaction() {
		mTx = txs.get();
		return this;
	}
	
	
	public static Transaction getTransaction() {
		if (mTx == null) {
			mTx = txs.get();
		}
		return mTx;
	}

}
