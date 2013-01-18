package org.eulerdb.kernel;

import javax.transaction.xa.XAException;


import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;


public class EdbThreadedTransactionalGraph extends EdbTransactionalGraph implements ThreadedTransactionalGraph{
	
	 protected final ThreadLocal<Transaction> tx = new ThreadLocal<Transaction>() {
	        protected Transaction initialValue() {
	            return null;
	        }
	    };

	public EdbThreadedTransactionalGraph(String path) {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	public EdbTransactionalGraph startThreadTransaction() {
		
		generateXid();
		super.mTx = xaEnv.beginTransaction(null, null);
		super.xaEnv.setXATransaction(xid, mTx);
		
		this.tx.set(mTx);
		
		return this;
	}

}