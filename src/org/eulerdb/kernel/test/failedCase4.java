package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.helper.EdbHelper;
import org.junit.Assert;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class failedCase4 {

	public static void main(String[] args) {
		EdbGraph graph = new EdbGraph("./temp/failedCase4");
		Vertex a = graph.addVertex(null);
		Vertex b = graph.addVertex(null);
		Edge edge = graph.addEdge(null, a, b, "knows");

		graph.removeVertex(a);
		graph.removeEdge(edge);
		
		if (graph.getFeatures().supportsEdgeIteration) {
			Assert.assertEquals(0, EdbHelper.count(graph.getEdges()));
		}
		if (graph.getFeatures().supportsVertexIteration) {
			Assert.assertEquals(1, EdbHelper.count(graph.getVertices()));
		}

		graph.shutdown();
	}
}
