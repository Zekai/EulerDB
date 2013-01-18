package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

public class WorkloadTest {

	public static void main(String[] args) {

		String path = "./temp/workload";

		FileHelper.deleteDir(path);

		EdbGraph g = new EdbGraph(path);
		
		for(int i=0;i<10000;i++)
		{
			System.out.println(i);
			EdbVertex v = (EdbVertex) g.addVertex(i);
		}
		
		g.nontransactionalCommit();

	}
}
