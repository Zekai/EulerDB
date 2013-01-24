package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbTransactionalGraph;
import org.junit.Assert;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class BasicTest3 {
	//failed case, cross update
	public static void main(String[] args) {
		EdbTransactionalGraph graph = new EdbTransactionalGraph(
				"./temp/basicTest3");
		Edge edge = graph.addEdge(null, graph.addVertex(null),
				graph.addVertex(null), "test");
		graph.startTransaction();
		edge.setProperty("transaction-2", "failure");
		Assert.assertEquals("failure", edge.getProperty("transaction-2"));
		graph.stopTransaction(Conclusion.FAILURE);
		System.out.println(edge.getProperty("transaction-2"));
	}
}
