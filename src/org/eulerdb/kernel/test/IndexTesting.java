package org.eulerdb.kernel.test;

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
		//Vertex v3 = g.addVertex(3);
		//Vertex v4 = g.addVertex(4);*/

		// add nodes properties
		v1.setProperty(name, "Marek");
		v1.setProperty(gender, "Female");
		//v1.setProperty(location, "california");
		v2.setProperty(name, "Mock");
		v2.setProperty(gender, "Male");
		//v2.setProperty(location, "new york");
		
		//for(String s:v1.getPropertyKeys()){
		//	System.out.println(v2.getProperty(s));
		//}
		//v3.setProperty(name, "Macka");
		/*v3.setProperty(gender, "Female");
		v4.setProperty(name, "Andrejka");
		v4.setProperty(gender, "Female");

		// add some edges
		g.addEdge(null, v1, v2, "isFriedOf");
		g.addEdge(null, v2, v1, "isFriedOf");

		g.addEdge(null, v1, v3, "dates");
		g.addEdge(null, v3, v1, "dates");

		g.addEdge(null, v2, v4, "isMarriedTo");
		g.addEdge(null, v4, v2, "isMarriedTo");

		g.addEdge(null, v1, v4, "isFriedOf");
		g.addEdge(null, v4, v1, "isFriedOf");

		g.addEdge(null, v3, v4, "isFriedOf");
		g.addEdge(null, v4, v3, "isFriedOf");*/

		// index properties
		g.getIndexedKeys(Vertex.class);
		System.out.println("done");
	}
}
