package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;

public class WorkloadTest {

	public static void main(String[] args) {

		String path = "./temp/WorkloadTest";

		FileHelper.deleteDir(path);
		Long s = System.nanoTime();
		EdbTransactionalGraph g = new EdbTransactionalGraph(path);
		EdbVertex v0 = (EdbVertex) g.addVertex(0);
		EdbVertex v1 = (EdbVertex) g.addVertex(1);
		
		for(int i=2;i<10000;i++)
		{
			System.out.println(i);
			EdbVertex v = (EdbVertex) g.addVertex(i);
			g.addEdge(null, v, g.getVertex(i-1), "likes");
			g.addEdge(null, v, g.getVertex(i-2), "hates");
		}
		
		g.stopTransaction(Conclusion.SUCCESS);
		Long e = System.nanoTime();
		System.out.println("nano time"+ (e-s));

	}
}