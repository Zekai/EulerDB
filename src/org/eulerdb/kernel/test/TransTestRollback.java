package org.eulerdb.kernel.test;

import javax.transaction.xa.XAException;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class TransTestRollback {
	public static void main(String[] args) {

		String path = "./temp/TransTestRollback";

		testGenerating(path);
		//testloading(path);
	}
	
	public static void testGenerating(String path){
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

		g.addEdge(1, v1, v2, "likes");
		g.addEdge(1, v2, v3, "hates");
		g.addEdge(1, v2, v4, "hates");
		g.addEdge(1, v4, v1, "likes");
		
		g.stopTransaction(Conclusion.SUCCESS);
		
		g.startTransaction();
		
		System.out.println("g:" + g.mNodePairs.count());
		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");
		}
		
		/*
		try {
			g.prepare();
			//g.commit();
			g.rollback();
		} catch (XAException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		g.stopTransaction(Conclusion.FAILURE);
		
		testloading(path);
		
		System.out.println("g:" + g.mNodePairs.count());
		System.out.println("=====================================================");
		g.shutdown();
	}

	public static void testloading(String path) {
		System.out.println("=====================================================");

		EdbTransactionalGraph g = new EdbTransactionalGraph(path);

		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");

			for (Vertex u : ((EdbVertex) v).getVertices(Direction.IN, null)) {
				System.out.println("     " + u.getId() + " connects to "
						+ v.getId());
			}

		}

		System.out.println("\nAll edges");
		for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId() + " "
					+ ((EdbEdge) e).getLabel() + " "
					+ e.getVertex(Direction.OUT).getId());
		}
	}
}
