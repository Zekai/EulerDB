package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

public class BasicTestQuery {
public static void main(String[] args) {
		
		String path = "./temp/BasicTestQuery";
		
		FileHelper.deleteDir(path);
		
		EdbGraph g = new EdbGraph(path);

		EdbVertex v1 = (EdbVertex) g.addVertex(1);

		EdbVertex v2 = (EdbVertex) g.addVertex(2);
		
		
		EdbVertex v3 = (EdbVertex) g.addVertex(3);

		EdbVertex v4 = (EdbVertex) g.addVertex(4);
		
		
		g.addEdge(0.4f, v1, v2, "likes");
		g.addEdge(0.4f, v1, v3, "hates");
		g.addEdge(0.4f, v1, v4, "hates");
		
		
		/*for(Edge e:g.getEdges())
		{
			System.out.println(e.getVertex(Direction.IN).getId()+" "+e.getLabel() + " " + e.getVertex(Direction.OUT).getId());
		}*/
		
		for(Vertex v:v1.query().labels("hates").vertices())
		{
			System.out.println(v1.getId() + " hates " + v.getId());
		}

		
		g.nontransactionalCommit();
	}
}
