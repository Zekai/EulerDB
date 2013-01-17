package org.eulerdb.kernel.test;

import java.util.Collection;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

public class BasicTest2 {
	public static void main(String[] args) {

		String path = "./temp/BasicTest2";

		FileHelper.deleteDir(path);

		EdbGraph g = new EdbGraph(path);

		EdbVertex v1 = new EdbVertex(1);
		g.addVertex(v1);
		
		EdbVertex v2 = new EdbVertex(2);
		g.addVertex(v2);
		
		EdbVertex v3 = new EdbVertex(3);
		g.addVertex(v3);
		
		g.addEdge(0.4f, v1, v2, "likes");
		
		g.addEdge(0.4f, v2, v3, "likes");
		
		for(int i = 3;i<1000000;i++){
			System.out.println(i);
			EdbVertex v = new EdbVertex(i);
			g.addVertex(v);
		}
		
		System.gc();
		
	}
}
