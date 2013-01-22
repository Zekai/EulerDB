package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eulerdb.kernel.iterator.IteratorFactory;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.google.common.base.Function;
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

	//public static Long Id = 0L;//FIXME put the autoincreament storage
	
	private static final AtomicInteger uniqueId = new AtomicInteger(0);
	
	protected transient static EdbStorage mStorage = null;

	private String mId;

	private transient final static List<String> sBlackList = Arrays
			.asList(new String[] { "id" });
	// private Multimap<String, EdbVertex> mInRelationMap;
	// private Multimap<String, EdbVertex> mOutRelationMap;

	private Multimap<EdbVertex, EdbEdge> mInEdges;
	private Multimap<EdbVertex, EdbEdge> mOutEdges;
	private Map<String, Object> mProps;

	public EdbVertex(Object id) {

		mId = id == null ? String.valueOf(uniqueId.getAndIncrement()) : String.valueOf(id);
		// mInRelationMap = HashMultimap.create();
		// mOutRelationMap = HashMultimap.create();
		
		if(mStorage==null) mStorage = EdbStorage.getInstance("",true);

		mInEdges = HashMultimap.create();// new LinkedList<EdbEdge> ();
		mOutEdges = HashMultimap.create();// new LinkedList<EdbEdge> ();\
		mProps = new HashMap<String, Object>();
	}

	@Override
	public String getId() {

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

	// FIXME property is currently not saved.
	@Override
	public void setProperty(String arg0, Object arg1) {
		if (sBlackList.contains(arg0))
			throw new IllegalArgumentException(arg0
					+ " is not allowed to be used as property name");
		mProps.put(arg0, arg1);
		mStorage.store(storeType.VERTEX, this);//FIXME this code make it nontransactional
	}

	@Override
	public Iterable<Edge> getEdges(Direction arg0, String... arg1) {

		if (arg1 == null || arg1.length == 0) {
			if (arg0 == Direction.IN) {
				return IteratorFactory.getEdgeIterator(mInEdges.values()
						.iterator());// new
										// EdbEdgeIteratorFromCollection(mInEdges.iterator());
			} else if (arg0 == Direction.OUT) {
				return IteratorFactory.getEdgeIterator(mOutEdges.values()
						.iterator());// return new
										// EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} else if (arg0 == Direction.BOTH) {
				List<EdbEdge> total = new ArrayList<EdbEdge>();//
				total.addAll(mInEdges.values());
				total.addAll(mOutEdges.values());
				return IteratorFactory.getEdgeIterator(total.iterator());// return
																			// new
																			// EdbEdgeIteratorFromCollection(total.iterator());
			}
		} else {

			final String relation = arg1[0];// FIXME should allows more than one
											// labels
			Predicate<Edge> relationFilter = new Predicate<Edge>() {
				public boolean apply(Edge e) {
					return e.getLabel().equals(relation);
				}
			};

			if (arg0 == Direction.IN) {
				return IteratorFactory.getEdgeIterator(Collections2.filter(
						mInEdges.values(), relationFilter).iterator());// new
																		// EdbEdgeIteratorFromCollection(mInEdges.iterator());
			} else if (arg0 == Direction.OUT) {
				return IteratorFactory.getEdgeIterator(Collections2.filter(
						mOutEdges.values(), relationFilter).iterator());// return
																		// new
																		// EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} else if (arg0 == Direction.BOTH) {
				List<EdbEdge> total = new ArrayList<EdbEdge>();//
				total.addAll(mInEdges.values());
				total.addAll(mOutEdges.values());
				return IteratorFactory.getEdgeIterator(Collections2.filter(
						total, relationFilter).iterator());// return new
															// EdbEdgeIteratorFromCollection(total.iterator());
			}
		}

		return null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.tinkerpop.blueprints.Vertex#getVertices(com.tinkerpop.blueprints.
	 * Direction, java.lang.String[])
	 * 
	 * the mInRelationMap.values().iterator() might contain duplicate keys when
	 * there are more than one edge between two vertices this is what it
	 * supposed to be, don't fix it
	 */
	@Override
	public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {

		if (arg1 == null || arg1.length == 0) {

			if (arg0 == Direction.IN)
				return IteratorFactory.getVertexIterator(mInEdges.keys()
						.iterator());// new
										// EdbVertexIteratorFromCollection(mInRelationMap.values().iterator());
			else if (arg0 == Direction.OUT)
				return IteratorFactory.getVertexIterator(mOutEdges.keys()
						.iterator());// EdbVertexIteratorFromCollection(mOutRelationMap.values().iterator());
			else if (arg0 == Direction.BOTH) {
				List<EdbVertex> total = new ArrayList<EdbVertex>();//
				total.addAll(mInEdges.keys());
				total.addAll(mOutEdges.keys());
				return IteratorFactory.getVertexIterator(total.iterator());// new
																			// EdbVertexIteratorFromCollection(total.iterator());
			}

			throw new IllegalArgumentException("Wrong direction type: "
					+ arg0.toString());
		} else {
			final String relation = arg1[0];// FIXME should allows more than one
											// labels

			Predicate<Edge> relationFilter = new Predicate<Edge>() {
				public boolean apply(Edge e) {
					return e.getLabel().equals(relation);
				}
			};

			Function<EdbEdge, EdbVertex> inF = new Function<EdbEdge, EdbVertex>() {
				@Override
				public EdbVertex apply(final EdbEdge edge) {
					return (EdbVertex) edge.getVertex(Direction.OUT);// that is
																		// the
																		// from
																		// vertex
				}
			};

			Function<EdbEdge, EdbVertex> outF = new Function<EdbEdge, EdbVertex>() {
				@Override
				public EdbVertex apply(final EdbEdge edge) {
					return (EdbVertex) edge.getVertex(Direction.IN);// that is
																	// the to
																	// vertex
				}
			};

			if (arg0 == Direction.IN) {
				return IteratorFactory.getVertexIterator(Collections2
						.transform(
								Collections2.filter(mInEdges.values(),
										relationFilter), inF).iterator());// new
				// EdbEdgeIteratorFromCollection(mInEdges.iterator());
			} else if (arg0 == Direction.OUT) {
				return IteratorFactory.getVertexIterator(Collections2
						.transform(
								Collections2.filter(mOutEdges.values(),
										relationFilter), outF).iterator());// return
				// new
				// EdbEdgeIteratorFromCollection(mOutEdges.iterator());
			} else if (arg0 == Direction.BOTH) {
				List<EdbVertex> total = new ArrayList<EdbVertex>();//
				total.addAll(Collections2.transform(
						Collections2.filter(mInEdges.values(), relationFilter),
						inF));
				total.addAll(Collections2.transform(
						Collections2.filter(mOutEdges.values(), relationFilter),
						outF));
				return IteratorFactory.getVertexIterator(total.iterator());
			}
		}
		return null;

	}

	@Override
	public Query query() {
		return new DefaultQuery(this);
	}

	/**
	 * see https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model for
	 * the in/out relation
	 * 
	 * @param e
	 */
	void addInEdge(EdbEdge e) {
		// mInRelationMap.put(e.getLabel(),
		// (EdbVertex)e.getVertex(Direction.IN));
		mInEdges.put((EdbVertex) e.getVertex(Direction.OUT), e);
	}

	void addOutEdge(EdbEdge e) {
		// mOutRelationMap.put(e.getLabel(), (EdbVertex) e.getToVertex());
		mOutEdges.put((EdbVertex) e.getVertex(Direction.IN), e);
	}

	void removeInEdge(EdbEdge e) {
		mInEdges.remove((EdbVertex) e.getVertex(Direction.OUT), e);
		// mInRelationMap.remove(e.getLabel(), e.getVertex(Direction.IN));

	}

	void removeOutEdge(EdbEdge e) {
		mOutEdges.size();
		for (EdbEdge e1 : mOutEdges.get((EdbVertex) e.getVertex(Direction.IN))) {
			System.out.println(e1.getId());
		}
		mOutEdges.remove((EdbVertex) e.getVertex(Direction.IN), e);
		mOutEdges.size();
		// mOutRelationMap.remove(e.getLabel(), e.getVertex(Direction.OUT));

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (mId == null ? 0 : mId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj.getClass() == getClass()))
			return false;
		EdbVertex other = (EdbVertex) obj;
		if (!mId.equals(other.getId()))
			return false;
		return true;
	}

	/*
	 * //Following 2 functions duplicate the Query feature, //consider removing
	 * them and the relationmap public Collection<EdbVertex> getOut(String
	 * relation) { return mOutRelationMap.get(relation); }
	 * 
	 * public Collection<EdbVertex> getIn(String relation) { return
	 * mInRelationMap.get(relation); }
	 */

	public List<String> getInRelationWith(Vertex u) {
		List<String> relations = new ArrayList<String>();
		for (EdbEdge e : mInEdges.get((EdbVertex) u)) {
			relations.add(e.getLabel());
		}
		return relations;
	}

	public List<String> getOutRelationWith(Vertex u) {
		List<String> relations = new ArrayList<String>();
		for (EdbEdge e : mOutEdges.get((EdbVertex) u)) {
			relations.add(e.getLabel());
		}
		return relations;
	}

}
