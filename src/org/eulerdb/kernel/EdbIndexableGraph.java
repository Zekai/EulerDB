package org.eulerdb.kernel;


import java.util.Set;

import org.eulerdb.kernel.iterator.EdbIterableFromDatabase;
import org.eulerdb.kernel.storage.EdbSecondaryCursor;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;

/*
 * KeyIndexableGraph: auto index base on the property name
 * IndexableGraph: manual index, user needs to put into the vertex by themselves 
 */

public class EdbIndexableGraph extends EdbGraph implements KeyIndexableGraph{
	
	public EdbIndexableGraph(String path){
		super(path,false,true);
	}

	public EdbIndexableGraph(String path,boolean isTransactional,boolean autoIndex) {
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
		
		Function<String, Vertex> idToObject = new Function<String, Vertex>() {
			  public Vertex apply(String id) {
			    return (Vertex) mStorage.getObj(storeType.VERTEX, null, id);
			  }
			};
		
			Iterable<String> id = new EdbIterableFromDatabase(new EdbSecondaryCursor(mStorage.getSecondaryCursor(storeType.NODEPROPERTY, null, key, value),value));
			Iterable<Vertex> eit= (Iterable<Vertex>) Iterables.transform(id, idToObject);
			return eit;
	}
	
	/**
	 * we are overriding the get Edges here to make use of the index
	 */
	@Override
	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		final String key = arg0;
		final String value = (String) arg1;
		
		Function<String, Edge> idToObject = new Function<String, Edge>() {
			  public Edge apply(String id) {
			    return (Edge) mStorage.getObj(storeType.EDGE, null, id);
			  }
			};
			
		Iterable<String> id = (Iterable<String>) new EdbIterableFromDatabase(new EdbSecondaryCursor(mStorage.getSecondaryCursor(storeType.EDGEPROPERTY, null, key, value),value));;
		Iterable<Edge> eit= (Iterable<Edge>) Iterables.transform(id, idToObject);
		
		return eit;
	}
	
}
