package org.eulerdb.kernel.index;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Element;


public class EdbIndex implements Index<Element>{

	@Override
	public long count(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CloseableIterable<Element> get(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<Element> getIndexClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIndexName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(String arg0, Object arg1, Element arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CloseableIterable<Element> query(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(String arg0, Object arg1, Element arg2) {
		// TODO Auto-generated method stub
		
	}

}
