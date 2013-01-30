package org.eulerdb.kernel;

import java.util.Set;

import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;

public class EdbIndexablGraph extends EdbGraph implements KeyIndexableGraph{
	

	public EdbIndexablGraph(String path) {
		super(path);
		// TODO Auto-generated constructor stub
	}

	@Override
	public <T extends Element> void createKeyIndex(String key, Class<T> type) {
		if(type.equals(Vertex.class)||type.equals(EdbVertex.class))
		{
			mStorage.openSecondary(key,storeType.NODEPROPERTY);
		}
		else if(type.equals(Edge.class)||type.equals(EdbEdge.class))
		{
			mStorage.openSecondary(key,storeType.EDGEPROPERTY);
		}
	}

	@Override
	public <T extends Element> void dropKeyIndex(String key, Class<T> type) {
		if(type.equals(Vertex.class)||type.equals(EdbVertex.class))
		{
			 mStorage.deleteSecondary(storeType.NODEPROPERTY, null, key);
		}
		else if(type.equals(Edge.class)||type.equals(EdbEdge.class))
		{
			mStorage.deleteSecondary(storeType.EDGEPROPERTY, null, key);
		}
		
	}

	@Override
	public <T extends Element> Set<String> getIndexedKeys(Class<T> type) {
		if(type.equals(Vertex.class)||type.equals(EdbVertex.class))
		{
			return mStorage.getKeys(storeType.NODEPROPERTY);
		}
		else if(type.equals(Edge.class)||type.equals(EdbEdge.class))
		{
			return mStorage.getKeys(storeType.EDGEPROPERTY);
		}
		return null;
	}

	

}
