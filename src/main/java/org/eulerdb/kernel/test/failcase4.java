package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.storage.EdbManager;
import org.junit.Assert;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class failcase4 {
	public static void main(String[] args) {
		EdbManager.deleteEnv("graph");
		EdbTransactionalGraph graph = new EdbTransactionalGraph("graph", true,
				false);
		Vertex v = graph.addVertex(null);
		Object id = v.getId();
		v.setProperty("name", "marko");
		graph.stopTransaction(Conclusion.SUCCESS);

		v = graph.getVertex(id);
		Assert.assertNotNull(v);
		Assert.assertEquals(v.getProperty("name"), "marko");
		v.setProperty("age", 30);
		Assert.assertEquals(v.getProperty("age"), 30);
		graph.stopTransaction(Conclusion.FAILURE);

		v = graph.getVertex(id);
		Assert.assertNull(v.getProperty("age"));

		graph.shutdown();
	}
}
