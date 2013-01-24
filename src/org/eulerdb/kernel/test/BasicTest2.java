package org.eulerdb.kernel.test;

import java.util.Collection;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;
import org.junit.Assert;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

public class BasicTest2 {
	public static void main(String[] args) {
		//failed case, cross update
		String path = "./temp/BasicTest2";
		
		FileHelper.deleteDir(path);
		
		Graph graph = new EdbGraph(path);
        Vertex a = graph.addVertex(null);
        Vertex b  = graph.addVertex(null);
        Vertex c  =  graph.addVertex(null);
        for (Vertex vertex : graph.getVertices()) {
            graph.addEdge(null, vertex, a, "x");
            graph.addEdge(null, vertex, a, "y");
        }
        for (Vertex vertex : graph.getVertices()) {
        	//System.out.println(vertex.getId()+":"+BaseTest.count(vertex.getEdges(Direction.OUT)));
            //assertEquals(BaseTest.count(vertex.getEdges(Direction.OUT)), 2);
        	System.out.println(vertex.getId()+"-----");
        	for(Edge e:vertex.getEdges(Direction.OUT)){
        		System.out.println(e.getId());
        	}
        }

		/*FileHelper.deleteDir(path);

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
		
		System.out.println("\nAll edges");*/
		/*for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.OUT).getId());
		}*/
		
	}
}
