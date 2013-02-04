package org.eulerdb.kernel.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.eulerdb.kernel.EdbEdge;
import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.EdbVertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.gml.GMLReader;

public class BasicTestIO2 {
	public static void main(String[] args) {
		EdbGraph g = new EdbGraph("./temp/BasicTestIO2");
		InputStream in;
		try {
			in = new FileInputStream("./dataset/lesmiserables.gml");
			GMLReader.inputGraph(g, in);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			
		}
		
		System.out.println("All nodes:");
		for (Vertex v : g.getVertices()) {
			System.out.println(v.getId() + " : ");
			
			for(String s: v.getPropertyKeys()){
				System.out.println(s+":"+v.getProperty(s));
			}
			
			for (Vertex u: ((EdbVertex)v).getVertices(Direction.BOTH, null))
			{
				System.out.println("     "+u.getId()+" connects to "+ v.getId());
			}
			
		}
		
		System.out.println("\nAll edges");
		for (Edge e : g.getEdges()) {
			System.out.println(e.getVertex(Direction.IN).getId()+" "+((EdbEdge) e).getLabel()+" "+e.getVertex(Direction.OUT).getId());
		}

		
	}
}
