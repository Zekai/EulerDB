package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class BasicTestRelation {
	public static void main(String[] args) {

		String path = "./temp_BasicTestRelation";

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
		g.addEdge(0.4f, v2, v4, "hates");
		g.addEdge(0.4f, v4, v1, "likes");
		g.commit();
		for(String s: v4.getOutRelationWith(v1))
		{
			System.out.print(v4.getId() + " " + s +" "+v1.getId());
		}
		
	}
}
