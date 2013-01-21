package org.eulerdb.kernel.test;

import org.eulerdb.kernel.EdbTransactionalGraph;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;

public class ACIDTestIsolation2 {
	public static void main(String[] args) {
		
		FileHelper.deleteDir("./temp/ttg");

		Thread thread1 = new Thread(new Runnable(){

			@Override
			public void run() {
				EdbTransactionalGraph g =  new EdbTransactionalGraph("");
				g.startTransaction();
				EdbVertex v = (EdbVertex) g.addVertex(1);
				System.out.println(Thread.currentThread()+" adding vertex");
				
			}}, "thread1");
		Thread thread2 = new Thread(new Runnable(){

			@Override
			public void run() {
				EdbTransactionalGraph g =  new EdbTransactionalGraph("");
				g.startTransaction();
				EdbVertex v = (EdbVertex) g.addVertex(2);
				System.out.print("\n [thread2 ");
				for(Vertex u:g.getVertices()){
					System.out.print(" "+u.getId()+" ");
				}
				System.out.println("thread2]");
			}}, "thread2");
		//Start the threads
		thread1.start();
		thread2.start();
		try {
			//delay for one second
			Thread.currentThread().sleep(1000);
		} catch (InterruptedException e) {
		}
		//Display info about the main thread
		System.out.println(Thread.currentThread());
		
	}
}