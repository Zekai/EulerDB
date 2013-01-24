package org.eulerdb.kernel.test;

import java.util.HashSet;
import java.util.Set;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.helper.EdbHelper;
import org.junit.Assert;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class failedCase3 {
	//out of memory
	public static void main(String[] args) {
		Graph graph = new EdbGraph("./temp/failedCase3");
		int branchSize = 11;
		Vertex start = graph.addVertex(null);
		for (int i = 0; i < branchSize; i++) {
			Vertex a = graph.addVertex(null);
			graph.addEdge(null, start, a, "test1");
			for (int j = 0; j < branchSize; j++) {
				Vertex b = graph.addVertex(null);
				graph.addEdge(null, a, b, "test2");
				for (int k = 0; k < branchSize; k++) {
					Vertex c = graph.addVertex(null);
					graph.addEdge(null, b, c, "test3");
				}
			}
		}

		Assert.assertEquals(0, EdbHelper.count(start.getEdges(Direction.IN)));
		Assert.assertEquals(branchSize, EdbHelper.count(start.getEdges(Direction.OUT)));
		for (Edge e : start.getEdges(Direction.OUT)) {

			Assert.assertEquals(branchSize,
					EdbHelper.count(e.getVertex(Direction.IN).getEdges(Direction.OUT)));
			Assert.assertEquals(1,
					EdbHelper.count(e.getVertex(Direction.IN).getEdges(Direction.IN)));
			for (Edge f : e.getVertex(Direction.IN).getEdges(Direction.OUT)) {

				Assert.assertEquals(branchSize, EdbHelper.count(f.getVertex(Direction.IN)
						.getEdges(Direction.OUT)));
				Assert.assertEquals(1,
						EdbHelper.count(f.getVertex(Direction.IN).getEdges(Direction.IN)));
				for (Edge g : f.getVertex(Direction.IN).getEdges(Direction.OUT)) {

					Assert.assertEquals(
							0,
							EdbHelper.count(g.getVertex(Direction.IN).getEdges(
									Direction.OUT)));
					Assert.assertEquals(
							1,
							EdbHelper.count(g.getVertex(Direction.IN).getEdges(
									Direction.IN)));
				}
			}
		}

		int totalVertices = 0;
		for (int i = 0; i < 4; i++) {
			totalVertices = totalVertices + (int) Math.pow(branchSize, i);
		}

		if (graph.getFeatures().supportsVertexIteration) {

			Set<Vertex> vertices = new HashSet<Vertex>();
			for (Vertex v : graph.getVertices()) {
				vertices.add(v);
			}

		}

		if (graph.getFeatures().supportsEdgeIteration) {

			Set<Edge> edges = new HashSet<Edge>();
			for (Edge e : graph.getEdges()) {
				edges.add(e);
			}

		}
		graph.shutdown();
		System.out.println("Done======");

	}
}
