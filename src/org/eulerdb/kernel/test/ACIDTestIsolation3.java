package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class ACIDTestIsolation3 {
	public static void main(String[] args) {

		String path = "./temp/ACIDTestIsolation3";
		generate(path);
		
		EdbTransactionalGraph g1 =  new EdbTransactionalGraph(path);
		//EdbTransactionalGraph g2 = new EdbTransactionalGraph(path);
		
		g1.startTransaction();
		EdbVertex vv = (EdbVertex) g1.getVertex(1);
		//g2.startTransaction();
		g1.removeVertex(vv);
		
		for(Vertex v:g1.getVertices()){
			System.out.println(v.getId());
		}
		//System.out.println("g2 reading "+ g1.getVertex(1).getId());
		//g2.stopTransaction(Conclusion.SUCCESS);
		g1.stopTransaction(Conclusion.SUCCESS);
		
		
		
		
	}
	
	public static void generate(String path){
		
		FileHelper.deleteDir(path);
		EdbTransactionalGraph g = new EdbTransactionalGraph(path);
		g.startTransaction();
		
		
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
		
		
		
		g.removeVertex(v1);
		
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
		g.stopTransaction(Conclusion.SUCCESS);
	}
}