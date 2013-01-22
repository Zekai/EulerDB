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
		
		String path = "./temp/BasicTest";
		
		FileHelper.deleteDir(path);
		
		EdbGraph g = new EdbGraph(path);
		
		EdbVertex v1 = (EdbVertex) g.addVertex(1);
		v1.setProperty("name", "AAAAA");
		v1.setProperty("age", 25);
		v1.setProperty("gender", "male");
		
		EdbVertex v2 = (EdbVertex) g.addVertex(2);
		EdbVertex v3 = (EdbVertex) g.addVertex(3);
		EdbVertex v4 = (EdbVertex) g.addVertex(4);
		EdbVertex vx = (EdbVertex) g.getVertex(1);
		for(String s: vx.getPropertyKeys()){
			System.out.println(s+":"+vx.getProperty(s));
		}
		
		g.addEdge(0, v1, v2, "likes");
		g.addEdge(0, v2, v3, "hates");
		g.addEdge(0, v2, v4, "hates");
		g.addEdge(0, v4, v1, "likes");
		
		
		
		g.removeVertex(v2);
		
		
		
		g.nontransactionalCommit();
		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");
			
			for (Vertex u: ((EdbVertex)v).getVertices(Direction.IN, null))
			{
				System.out.println("     "+u.getId()+" connects to "+ v.getId());
			}
			
		}
		
		System.out.println("\nAll edges");
		for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.OUT).getId());
		}
		
	}
}
