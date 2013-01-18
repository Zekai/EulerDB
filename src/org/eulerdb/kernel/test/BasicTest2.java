package org.eulerdb.kernel.test;

import java.util.Collection;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

public class BasicTest2 {
	public static void main(String[] args) {

		String path = "./temp/BasicTest2";

		FileHelper.deleteDir(path);

		EdbGraph g = new EdbGraph(path);

		EdbVertex v1 = (EdbVertex) g.addVertex(1);
		
		EdbVertex v2 = (EdbVertex) g.addVertex(2);
		
		EdbVertex v3 = (EdbVertex) g.addVertex(3);
		
		for(int i = 3;i<1000000;i++){
			System.out.println(i);
			EdbVertex v = (EdbVertex) g.addVertex(i);
			g.addEdge("", v1, v, "knows");
		}
		
		System.gc();
		
		g.addEdge(1, v1, v2, "likes");
		
		g.addEdge(1, v2, v3, "likes");
		
		System.out.println("\nAll edges");
		/*for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.OUT).getId());
		}*/
		
	}
}
