package org.eulerdb.kernel.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;

public class BasicTestIO {

	public static void main(String[] args) {

		String name = "BasicTest";

		FileHelper.deleteDir(name);

		EdbGraph g = new EdbGraph(name);

		EdbVertex v1 = (EdbVertex) g.addVertex(1);;
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

		g.addEdge(0.4f, v1, v2, "likes");
		g.addEdge(0.4f, v2, v3, "hates");
		g.addEdge(0.4f, v2, v4, "hates");
		g.addEdge(0.4f, v4, v1, "likes");
		
		
		GraphMLWriter writer = new GraphMLWriter(g);
		writer.setNormalize(true);
		FileOutputStream fis = null;
		try {
			fis = new FileOutputStream("./temp/output.xml");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			writer.outputGraph(fis);
			fis.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
