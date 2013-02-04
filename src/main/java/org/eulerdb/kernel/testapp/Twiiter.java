package org.eulerdb.kernel.testapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eulerdb.kernel.EdbGraph;
import org.eulerdb.kernel.helper.FileHelper;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;

public class Twiiter {

	public static void main(String[] args) {
		FileHelper.deleteDir("./temp/twitter");
		EdbGraph graph = new EdbGraph("./temp/twitter");
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("./dataset/TwitterCrawl.txt"));
			String raw_l;
			int i = 0;
			while ((raw_l = br.readLine()) != null) {
				i++;
				System.out.println(i+":"+raw_l);
				String[] r = check(raw_l);
				if(r!=null){
					Vertex v = graph.getVertex(r[0]);
					if(v==null) v= graph.addVertex(r[0]);
					
					Vertex u = graph.getVertex(r[1]);
					if(u==null)u= graph.addVertex(r[1]);
					
					Edge e = graph.addEdge(null, v, u, "NA");
					e.setProperty("weight", r[2]);
				}
			}
			
			graph.nontransactionalCommit();

		} catch (Exception e) {
			System.err.println("Error:" + e.getMessage());
		}
		
		GraphMLWriter writer = new GraphMLWriter(graph);
		writer.setNormalize(true);
		FileOutputStream fis = null;
		try {
			fis = new FileOutputStream("./app/output.xml");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			writer.outputGraph(fis);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static String[] check(String line) {
		// "jack" -> "VirginAmerica" [weight=21];
		String pattern = "\"(\\w+)\" -> \"(\\w+)\" \\[weight=(\\d+)\\]";
		String[] result = new String[3];
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(line);
		if (m.find()) {
			//System.out.print(m.group(0));
			result[0] =  m.group(1);
			result[1]= m.group(2);
			result[2] =  m.group(3);
			return result;
		} else {
			System.out.println("NO MATCH");
		}
		return null;
	}
}
