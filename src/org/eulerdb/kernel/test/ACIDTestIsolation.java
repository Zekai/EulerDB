package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class ACIDTestIsolation {
	
	
	public static void main(String[] args){
		String path = "./temp/tt";
		
		testGenerating(path);
		
		EdbTransactionalGraph g1 = new EdbTransactionalGraph(path);
		for(Vertex u:g1.getVertices()){
			System.out.println(u.getId());
		}
		g1.startTransaction();
		Vertex v = g1.getVertex(1);
		
		EdbTransactionalGraph g2 = new EdbTransactionalGraph(path);
		g2.startTransaction();
		g2.removeVertex(v);
		
	}
	
	public static void testGenerating(String path) {
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
		for (String s : vx.getPropertyKeys()) {
			System.out.println(s + ":" + vx.getProperty(s));
		}
		
		g.stopTransaction(Conclusion.SUCCESS);

		g.shutdown();
	}

}
