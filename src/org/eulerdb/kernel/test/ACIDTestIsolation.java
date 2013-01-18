package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbThreadedTransactionalGraph;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class ACIDTestIsolation {
	public static void main(String[] args) {

		String path = "./temp/ACIDTestIsolation";
		generate(path);
		
		EdbThreadedTransactionalGraph g =  new EdbThreadedTransactionalGraph(path);
		EdbTransactionalGraph g1 = (EdbTransactionalGraph) g.startThreadTransaction();
		EdbTransactionalGraph g2 = (EdbTransactionalGraph) g.startThreadTransaction();
		//g1.startTransaction();
		EdbVertex vv = (EdbVertex) g1.getVertex(1);
		g1.removeVertex(vv);
		//g2.startTransaction();
		System.out.println("g2 reading "+ g2.getVertex(1).getId());
		g2.stopTransaction(Conclusion.SUCCESS);
		g1.stopTransaction(Conclusion.SUCCESS);
		
		
		
		
	}
	
	public static void generate(String path){
		
		FileHelper.deleteDir(path);
		EdbTransactionalGraph g = new EdbTransactionalGraph(path);
		
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

		g.addEdge(0, v1, v2, "likes");
		g.addEdge(0, v2, v3, "hates");
		g.addEdge(0, v2, v4, "hates");
		g.addEdge(0, v4, v1, "likes");
		
		g.stopTransaction(Conclusion.SUCCESS);
	}
}