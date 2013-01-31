package org.eulerdb.kernel;

import java.util.Set;

import org.eulerdb.kernel.iterator.EdbIterableFromDatabase;
import org.eulerdb.kernel.iterator.EdbIterableFromSecondaryDatabase;
import org.eulerdb.kernel.storage.EdbCursor;
import org.eulerdb.kernel.storage.EdbSecondaryCursor;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EulerDBHelper;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;

/*
 * KeyIndexableGraph: auto index base on the property name
 * IndexableGraph: manual index, user needs to put into the vertex by themselves 
 */

public class EdbIndexablGraph extends EdbGraph implements KeyIndexableGraph{
	
	public EdbIndexablGraph(String path){
		super(path,false,false);
	}

	public EdbIndexablGraph(String path,boolean isTransactional,boolean autoIndex) {
		super(path,isTransactional,autoIndex);
	}

	@Override
	public <T extends Element> void createKeyIndex(String key, Class<T> type) {
		if(type.equals(Vertex.class)||type.equals(EdbVertex.class))
		{
			mStorage.createSecondaryIfNeed(storeType.NODEPROPERTY, key);
		}
		else if(type.equals(Edge.class)||type.equals(EdbEdge.class))
		{
			mStorage.createSecondaryIfNeed(storeType.EDGEPROPERTY, key);
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


	/**
	 * we are overriding the get vertices here to make use of the index
	 */
	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;
		
		return new EdbIterableFromSecondaryDatabase(new EdbSecondaryCursor(mStorage.getSecondaryCursor(storeType.NODEPROPERTY, null, key, value),value));
	}
	
	/**
	 * we are overriding the get Edges here to make use of the index
	 */
	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;
		
		return new EdbIterableFromSecondaryDatabase(new EdbSecondaryCursor(mStorage.getSecondaryCursor(storeType.NODEPROPERTY, null, key, value),value));
	}
	
}
