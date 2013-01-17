package org.eulerdb.kernel.test;


import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;


public class BasicTestRelation {
	public static void main(String[] args) {

		String path = "./temp/BasicTestRelation";

		FileHelper.deleteDir(path);

		EdbGraph g = new EdbGraph(path);

		EdbVertex v1 = new EdbVertex(1);
		g.addVertex(v1);

		EdbVertex v2 = new EdbVertex(2);
		g.addVertex(v2);

		EdbVertex v3 = new EdbVertex(3);
		g.addVertex(v3);

		EdbVertex v4 = new EdbVertex(4);
		g.addVertex(v4);

		g.addEdge(0.4f, v1, v2, "likes");
		g.addEdge(0.4f, v2, v3, "hates");
		g.addEdge(0.4f, v1, v4, "hates");
		g.addEdge(0.4f, v4, v1, "likes");
		g.nontransactionalCommit();
		for(String s: v4.getOutRelationWith(v1))
		{
			System.out.println(v4.getId() + " " + s +" "+v1.getId());
		}
		
		for(String s: v4.getInRelationWith(v1))
		{
			System.out.println(v2.getId() + " " + s +" "+v4.getId());
		}
		
		/*
		for (Edge e : v4.getEdges(Direction.IN)) {
			System.out.println(e.getVertex(Direction.OUT).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.IN).getId());
		}*/
		
	}
}
