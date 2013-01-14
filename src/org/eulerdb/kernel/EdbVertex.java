package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertex implements Vertex, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5653068124792462590L;

	private Integer mId;
	private Multimap<String, Integer> mInRelationMap;
	private Multimap<String, Integer> mOutRelationMap;

	// private List<Integer> mInEdges;
	// private List<Integer> mOutEdges;

	public EdbVertex(Integer id) {
		mId = id;
		mInRelationMap = HashMultimap.create();
		mOutRelationMap = HashMultimap.create();

		// mInEdges = new LinkedList<Integer> ();
		// mOutEdges = new LinkedList<Integer> ();
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

		/*
		if (arg0 == Direction.IN)
			return new EdbEdgeIterator(mInEdges.iterator());
		else
			return new EdbEdgeIterator(mOutEdges.iterator());
	*/
		return null;
	}

	@Override
	public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {
		/*
		 * if(arg0 == Direction.IN) return new
		 * EdbVertexIterator(mInEdges.iterator()); else return new
		 * EdbVertexIterator(mOutEdges.iterator());
		 */

		return null;

	}

	@Override
	public Query query() {
		// TODO Auto-generated method stub
		return null;
	}

	public void addInEdge(EdbEdge e) {
		mInRelationMap.put(e.getRelation(), (Integer) e.getToVertex().getId());

	}

	public void addOutEdge(EdbEdge e) {
		mOutRelationMap.put(e.getRelation(), (Integer) e.getToVertex().getId());

	}
	
	public Collection<Integer> getOut(String relation) {
		return mOutRelationMap.get(relation);
	}

	public Collection<Integer> getIn(String relation) {
		return mInRelationMap.get(relation);
	}

}
