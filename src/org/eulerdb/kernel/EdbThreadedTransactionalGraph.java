package org.eulerdb.kernel;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;

public class EdbThreadedTransactionalGraph extends EdbTransactionalGraph implements  ThreadedTransactionalGraph{

	public EdbThreadedTransactionalGraph(String path) {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	public TransactionalGraph startThreadTransaction() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
