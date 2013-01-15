package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

public class BasicTest {

	public static void main(String[] args) {
		
		String path = "./temp";
		
		FileHelper.deleteDir(path);
		
		EdbGraph g = new EdbGraph(path);

		EdbVertex v1 = new EdbVertex(1);
		g.addVertex(v1);
		
		Query q = v1.query();

		EdbVertex v2 = new EdbVertex(2);
		g.addVertex(v2);
		
		
		EdbVertex v3 = new EdbVertex(3);
		g.addVertex(v3);

		EdbVertex v4 = new EdbVertex(4);
		g.addVertex(v4);
		
		
		g.addEdge(0.4f, v1, v2, "likes");
		g.addEdge(0.4f, v2, v3, "hates");
		g.addEdge(0.4f, v2, v4, "hates");
		g.addEdge(0.4f, v4, v1, "likes");

		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");
			
			for (Vertex u: ((EdbVertex)v).getVertices(Direction.IN, null))
			{
				System.out.println("     "+u.getId()+" connects to "+ v.getId());
			}
			
			
			for (Vertex u: ((EdbVertex) v).getOut("likes"))
			{
				System.out.println("   Who he like :" + u.getId());
			}
			for (Vertex u: ((EdbVertex) v).getIn("hates"))
			{
				System.out.println("   Who hate him :" + u.getId());
			}
		}
		
		System.out.println("\nAll edges");
		for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.OUT).getId());
		}
		g.commit();
	}
}
