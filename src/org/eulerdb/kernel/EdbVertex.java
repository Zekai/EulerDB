package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.eulerdb.kernel.berkeleydb.EdbKeyPairStore;
import org.eulerdb.kernel.iterator.IteratorFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
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
	private Multimap<String, EdbVertex> mInRelationMap;
	private Multimap<String, EdbVertex> mOutRelationMap;

	private List<EdbEdge> mInEdges;
	private List<EdbEdge> mOutEdges;

	public EdbVertex(Integer id) {
		
		mId = id;
		mInRelationMap = HashMultimap.create();
		mOutRelationMap = HashMultimap.create();

		mInEdges = new LinkedList<EdbEdge> ();
		mOutEdges = new LinkedList<EdbEdge> ();
	}

	@Override
	public Integer getId() {

		return mId;
	}

	@Override
	public Object getProperty(String arg0) {

		return null;
	}

	@Override
	public Set<String> getPropertyKeys() {

		return null;
	}

	@Override
	public Object removeProperty(String arg0) {

		return null;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {

	}

	@Override
	public Iterable<Edge> getEdges(Direction arg0, String... arg1) {

		Integer size =  arg1.length;
		if(size==0)
		{
			if (arg0 == Direction.IN)
			{		
				return IteratorFactory.getEdgeIterator(mInEdges.iterator());//new EdbEdgeIteratorFromCollection(mInEdges.iterator());
			}
			else if (arg0 == Direction.OUT)
			{
				return IteratorFactory.getEdgeIterator(mOutEdges.iterator());//return new EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} else if(arg0 == Direction.BOTH)
			{
				List<EdbEdge> total = mInEdges;
				total.addAll(mOutEdges);
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
				
				Collections2.filter(mInEdges, relationFilter);
				
			if (arg0 == Direction.IN)
			{		
				return IteratorFactory.getEdgeIterator(Collections2.filter(mInEdges, relationFilter).iterator());//new EdbEdgeIteratorFromCollection(mInEdges.iterator());
			}
			else if (arg0 == Direction.OUT)
			{
				return IteratorFactory.getEdgeIterator(Collections2.filter(mOutEdges, relationFilter).iterator());//return new EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} 
			else if(arg0 == Direction.BOTH)
			{
				List<EdbEdge> total = mInEdges;
				total.addAll(mOutEdges);
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
			return IteratorFactory.getVertexIterator(mInRelationMap.values().iterator());//new EdbVertexIteratorFromCollection(mInRelationMap.values().iterator());
		else if (arg0 == Direction.OUT)
			return IteratorFactory.getVertexIterator(mOutRelationMap.values().iterator());//EdbVertexIteratorFromCollection(mOutRelationMap.values().iterator());
		else if(arg0 == Direction.BOTH)
		{
			Collection<EdbVertex> total = mInRelationMap.values();
			total.addAll(mOutRelationMap.values());
			return IteratorFactory.getVertexIterator(total.iterator());//new EdbVertexIteratorFromCollection(total.iterator());
		}
		
		throw new IllegalArgumentException("Wrong direction type: " + arg0.toString() );

	}

	@Override
	public Query query() {
		return new DefaultQuery(this);
	}

	public void addInEdge(EdbEdge e) {
		mInRelationMap.put(e.getLabel(), (EdbVertex)e.getVertex(Direction.IN));
		mInEdges.add(e);
	}

	public void addOutEdge(EdbEdge e) {
		mOutRelationMap.put(e.getLabel(), (EdbVertex) e.getToVertex());
		mOutEdges.add(e);
	}
	
	public void removeInEdge(EdbEdge e) {
		mInEdges.remove(e);
		mInRelationMap.remove(e.getLabel(), e.getVertex(Direction.IN));

	}
	
	public void removeOutEdge(EdbEdge e) {
		mOutEdges.remove(e);

		mOutRelationMap.remove(e.getLabel(), e.getVertex(Direction.OUT));

	}
	
	
	//Following 2 functions duplicate the Query feature, 
	//consider removing them and the relationmap
	public Collection<EdbVertex> getOut(String relation) {
		return mOutRelationMap.get(relation);
	}

	public Collection<EdbVertex> getIn(String relation) {
		return mInRelationMap.get(relation);
	}
	
	public Collection<String> getInRelationWith(Vertex u)  {
		Multimap<EdbVertex,String> iInRelation = HashMultimap.create();
		Multimaps.invertFrom(mInRelationMap, iInRelation);
		return iInRelation.get((EdbVertex)u);
	}
	
	public Collection<String> getOutRelationWith(Vertex u)  {
		Multimap<EdbVertex,String> iOutRelation = HashMultimap.create();
		Multimaps.invertFrom(mOutRelationMap, iOutRelation);
		return iOutRelation.get((EdbVertex)u);
	}

}
