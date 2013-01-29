package org.eulerdb.kernel;

import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;

public class EdbIndexableGraph extends EdbGraph implements IndexableGraph,KeyIndexableGraph{

	public EdbIndexableGraph(String path) {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	public <T extends Element> Index<T> createIndex(String arg0, Class<T> arg1,
			Parameter... arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dropIndex(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T extends Element> Index<T> getIndex(String arg0, Class<T> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Index<? extends Element>> getIndices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Element> void createKeyIndex(String arg0, Class<T> arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T extends Element> void dropKeyIndex(String arg0, Class<T> arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
