package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.iterator.IteratorFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;

public class EdbVertex implements Vertex, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5653068124792462590L;

	private Integer mId;
	//private Multimap<String, EdbVertex> mInRelationMap;
	//private Multimap<String, EdbVertex> mOutRelationMap;

	private Multimap<EdbVertex,EdbEdge> mInEdges;
	private Multimap<EdbVertex,EdbEdge> mOutEdges;
	private Map<String, Object> mProps;

	public EdbVertex(Integer id) {
		
		mId = id;
		//mInRelationMap = HashMultimap.create();
		//mOutRelationMap = HashMultimap.create();

		mInEdges = HashMultimap.create();//new LinkedList<EdbEdge> ();
		mOutEdges = HashMultimap.create();//new LinkedList<EdbEdge> ();\
		mProps = new HashMap<String,Object>();
	}

	@Override
	public Integer getId() {

		return mId;
	}

	@Override
	public Object getProperty(String arg0) {

		return mProps.get(arg0);
	}

	@Override
	public Set<String> getPropertyKeys() {

		return mProps.keySet();
	}

	@Override
	public Object removeProperty(String arg0) {

		return mProps.remove(arg0);
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		mProps.put(arg0, arg1);
	}

	@Override
	public Iterable<Edge> getEdges(Direction arg0, String... arg1) {

		Integer size =  arg1.length;
		if(size==0)
		{
			if (arg0 == Direction.IN)
			{		
				return IteratorFactory.getEdgeIterator(mInEdges.values().iterator());//new EdbEdgeIteratorFromCollection(mInEdges.iterator());
			}
			else if (arg0 == Direction.OUT)
			{
				return IteratorFactory.getEdgeIterator(mOutEdges.values().iterator());//return new EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} else if(arg0 == Direction.BOTH)
			{
				List<EdbEdge> total = new ArrayList<EdbEdge>();//
				total.addAll(mOutEdges.values());
				total.addAll(mOutEdges.values());
				return IteratorFactory.getEdgeIterator(total.iterator());//return new EdbEdgeIteratorFromCollection(total.iterator());
			}
		}
		else
		{
			
			final String relation  = arg1[0];//FIXME should allows more than one labels
			Predicate<Edge> relationFilter = new Predicate<Edge>() {
				  public boolean apply(Edge e) {
				    return e.getLabel().equals(relation);
				  }
				};
				
				Collections2.filter(mInEdges.values(), relationFilter);
				
			if (arg0 == Direction.IN)
			{		
				return IteratorFactory.getEdgeIterator(Collections2.filter(mInEdges.values(), relationFilter).iterator());//new EdbEdgeIteratorFromCollection(mInEdges.iterator());
			}
			else if (arg0 == Direction.OUT)
			{
				return IteratorFactory.getEdgeIterator(Collections2.filter(mOutEdges.values(), relationFilter).iterator());//return new EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} 
			else if(arg0 == Direction.BOTH)
			{
				List<EdbEdge> total = new ArrayList<EdbEdge>();//
				total.addAll(mOutEdges.values());
				total.addAll(mOutEdges.values());
				return IteratorFactory.getEdgeIterator(Collections2.filter(total, relationFilter).iterator());//return new EdbEdgeIteratorFromCollection(total.iterator());
			}
		}
		
		return null;

	}

	/*
	 * (non-Javadoc)
	 * @see com.tinkerpop.blueprints.Vertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])
	 * 
	 * the mInRelationMap.values().iterator() might contain duplicate keys
	 * when there are more than one edge between two vertices
	 * this is what it supposed to be, don't fix it
	 */
	@Override
	public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {

		if (arg0 == Direction.IN)
			return IteratorFactory.getVertexIterator(mInEdges.keys().iterator());//new EdbVertexIteratorFromCollection(mInRelationMap.values().iterator());
		else if (arg0 == Direction.OUT)
			return IteratorFactory.getVertexIterator(mOutEdges.keys().iterator());//EdbVertexIteratorFromCollection(mOutRelationMap.values().iterator());
		else if(arg0 == Direction.BOTH)
		{
			Collection<EdbVertex> total = mInEdges.keys();
			total.addAll(mOutEdges.keys());
			return IteratorFactory.getVertexIterator(total.iterator());//new EdbVertexIteratorFromCollection(total.iterator());
		}
		
		throw new IllegalArgumentException("Wrong direction type: " + arg0.toString() );

	}

	@Override
	public Query query() {
		return new DefaultQuery(this);
	}

	/**
	 * see https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model for the in/out relation
	 * @param e
	 */
	public void addInEdge(EdbEdge e) {
		//mInRelationMap.put(e.getLabel(), (EdbVertex)e.getVertex(Direction.IN));
		mInEdges.put((EdbVertex)e.getVertex(Direction.OUT),e);
	}

	public void addOutEdge(EdbEdge e) {
		//mOutRelationMap.put(e.getLabel(), (EdbVertex) e.getToVertex());
		mOutEdges.put((EdbVertex)e.getVertex(Direction.IN),e);
	}
	
	public void removeInEdge(EdbEdge e) {
		mInEdges.remove((EdbVertex)e.getVertex(Direction.OUT),e);
		//mInRelationMap.remove(e.getLabel(), e.getVertex(Direction.IN));

	}
	
	public void removeOutEdge(EdbEdge e) {
		mOutEdges.remove((EdbVertex)e.getVertex(Direction.IN),e);

		//mOutRelationMap.remove(e.getLabel(), e.getVertex(Direction.OUT));

	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (mId==null?0:mId);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj.getClass() == getClass() ) )
			  return false;
		EdbVertex other = (EdbVertex) obj;
		if (mId != other.getId())
			return false;
		return true;
	}
	
	/*
	//Following 2 functions duplicate the Query feature, 
	//consider removing them and the relationmap
	public Collection<EdbVertex> getOut(String relation) {
		return mOutRelationMap.get(relation);
	}

	public Collection<EdbVertex> getIn(String relation) {
		return mInRelationMap.get(relation);
	}
	*/
	
	
	public List<String> getInRelationWith(Vertex u)  {
		List<String> relations = new ArrayList<String>();
		for (EdbEdge e : mInEdges.get((EdbVertex) u)) {
			relations.add(e.getLabel());
		}
		return relations;
	}
	
	public List<String> getOutRelationWith(Vertex u)  {
		List<String> relations = new ArrayList<String>();
		for (EdbEdge e : mOutEdges.get((EdbVertex) u)) {
			relations.add(e.getLabel());
		}
		return relations;
	}
	

}
