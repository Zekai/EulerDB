package org.eulerdb.kernel.test;

import java.util.Hashtable;
import java.util.Map;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbIndexablGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.event.EventIndexableGraph;

public class IndexTesting {
	public static void main(String[] args) {
		// add some nodes

		String path = "./temp/BasicTest";

		FileHelper.deleteDir(path);
		
		String name  ="name";
		String gender = "gender"; 
		String location = "location";

		EdbIndexablGraph g = new EdbIndexablGraph(path);
		Vertex v1 = g.addVertex(1);
		Vertex v2 = g.addVertex(2);
		Vertex v3 = g.addVertex(3);
		Vertex v4 = g.addVertex(4);

		// add nodes properties
		v1.setProperty(name, "Mark");
		//v1.setProperty(gender, "Male");
		//v1.setProperty(location, "california");
		//v2.setProperty(name, "Mark");
		//v2.setProperty(gender, "Female");
		//v2.setProperty(location, "new york");
		
		//for(String s:v1.getPropertyKeys()){
		//	System.out.println(v2.getProperty(s));
		//}
		//v3.setProperty(name, "Macka");
		//v3.setProperty(gender, "Female");
//		v4.setProperty(name, "Andrejka");
//		v4.setProperty(gender, "Female");
//
//		// add some edges
//		g.addEdge(null, v1, v2, "isFriedOf");
//		g.addEdge(null, v2, v1, "isFriedOf");
//
//		g.addEdge(null, v1, v3, "dates");
//		g.addEdge(null, v3, v1, "dates");
//
//		g.addEdge(null, v2, v4, "isMarriedTo");
//		g.addEdge(null, v4, v2, "isMarriedTo");
//
//		g.addEdge(null, v1, v4, "isFriedOf");
//		g.addEdge(null, v4, v1, "isFriedOf");
//
//		g.addEdge(null, v3, v4, "isFriedOf");
//		g.addEdge(null, v4, v3, "isFriedOf");

		// index properties
		/*System.out.println("gender: Female");
		for(Vertex vx: g.getVertices(gender, "Female"))
		{
			System.out.println(vx.getId());
		}*/
		
		Vertex x = g.getVertex(1);
		System.out.println(x.getProperty("name"));
		
		
		System.out.println("name: Mark");
		for(Vertex vy: g.getVertices(name, "Mark"))
		{
			System.out.println(vy.getId());
		}
		Vertex x1 = g.getVertex(1);
		System.out.println(x1.getProperty("name"));
		/*
		System.out.println("name: Macka");
		
		for(Vertex vx: g.getVertices(name, "Mark"))
		{
			System.out.println(vx.getId());
		}
		Vertex x3 = g.getVertex(2);
		System.out.println(x3.getProperty("name"));
		System.out.println("name: Macka");
		for(Vertex vx: g.getVertices(name, "Macka"))
		{
			System.out.println(vx.getId());
		}
		System.out.println("name: Macka");
		/*v1.setProperty(name, "Mark");
		for(Vertex vx: g.getVertices(name, "Mark"))
		{
			System.out.println(vx.getId());
		}
		
		Vertex x2 = g.getVertex(2);
		System.out.println(x2.getProperty("name"));*/
	}
}
