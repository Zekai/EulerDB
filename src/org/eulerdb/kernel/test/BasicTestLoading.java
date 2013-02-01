package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class BasicTestLoading {

	public static void main(String[] args) {
		
		String path = "./temp/BasicTest";
		
		System.out.println("loading a graph with 1m vertices");
		
		EdbGraph g =  new EdbGraph(path);

		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");
			
			for (Vertex u: ((EdbVertex)v).getVertices(Direction.IN))
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
