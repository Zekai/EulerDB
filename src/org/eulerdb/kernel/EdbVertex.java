package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.iterator.EdbEdgeIterableFromCollection;
import org.eulerdb.kernel.iterator.EdbVertexIterableFromCollection;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;
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

	private static final AtomicInteger uniqueId = new AtomicInteger(0);

	protected transient static EdbStorage mStorage = null;

	private String mId;

	private transient final static List<String> sBlackList = Arrays
			.asList(new String[] { "id" });
	// private Multimap<String, EdbVertex> mInRelationMap;
	// private Multimap<String, EdbVertex> mOutRelationMap;

	private List<String> mInEdges;
	private List<String> mOutEdges;
	private Map<String, Object> mProps;

	public EdbVertex(Object id) {

		mId = id == null ? String.valueOf(uniqueId.getAndIncrement()) : String
				.valueOf(id);
		// mInRelationMap = HashMultimap.create();
		// mOutRelationMap = HashMultimap.create();

		if (mStorage == null)
			mStorage = EdbStorage.getInstance();

		mInEdges = new CopyOnWriteArrayList<String>();
		mOutEdges = new CopyOnWriteArrayList<String>();
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
		mStorage.store(storeType.VERTEX, EdbTransactionalGraph.txs.get(), this);// FIXME
																				// this
																				// code
																				// make
																				// it
																				// nontransactional
	}
	
	private String getAdjacent(storeType type,String edgeId,Direction dir,String... arg1){
		int firstIndex = edgeId.indexOf(Common.SEPARATOR_VERTEX);
		int secondIndex = edgeId.lastIndexOf(Common.SEPARATOR_VERTEX);
		String fromId = edgeId.substring(0,firstIndex);
		String relation = edgeId.substring(firstIndex+1,secondIndex);
		String toId = edgeId.substring(secondIndex+1);
		
		String resultId = null;
		
		if(arg1==null||arg1.length==0){
			if (dir == Direction.IN) {
				resultId=(type==storeType.EDGE)?edgeId:toId;
			} else if (dir == Direction.OUT) {
				resultId=(type==storeType.EDGE)?edgeId:fromId;
			} 
		}
		else{
			List<String> relations = Arrays.asList(arg1);
			if(relations.contains(relation))
				if (dir == Direction.IN) {
					resultId=(type==storeType.EDGE)?edgeId:toId;
				} else if (dir == Direction.OUT) {
					resultId=(type==storeType.EDGE)?edgeId:fromId;
				} 
		}
		return resultId;
	}

	@Override
	public Iterable<Edge> getEdges(Direction arg0, String... arg1) {

		List<Edge> res = new ArrayList<Edge>();
		if (arg0 == Direction.IN) {
			
			for (String s : this.mInEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, null, r));
				}
			}
		} else if (arg0 == Direction.OUT) {
			for (String s : this.mOutEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, null, r));
				}
			}
		} else if (arg0 == Direction.BOTH) {
			for (String s : this.mInEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, null, r));
				}
			}
			for (String s : this.mOutEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, null, r));
				}
			}
		}
		return new EdbEdgeIterableFromCollection(res);
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

		List<Vertex> res = new ArrayList<Vertex>();
		if (arg0 == Direction.IN) {
			for (String s : this.mInEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.OUT, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, null, r));
				}
			}
		} else if (arg0 == Direction.OUT) {
			for (String s : this.mOutEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.IN, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, null, r));
				}
			}
		} else if (arg0 == Direction.BOTH) {
			for (String s : this.mInEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.OUT, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, null, r));
				}
			}
			for (String s : this.mOutEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.IN, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, null, r));
				}
			}
		}

		return new EdbVertexIterableFromCollection(res);

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
		mInEdges.remove(e);
		mInEdges.add((String) e.getId());
	}

	void addOutEdge(EdbEdge e) {
		// mOutRelationMap.put(e.getLabel(), (EdbVertex) e.getToVertex());
		mOutEdges.remove(e);
		mOutEdges.add((String) e.getId());
	}

	void removeInEdge(EdbEdge e) {
		mInEdges.remove(e);
		// mInRelationMap.remove(e.getLabel(), e.getVertex(Direction.IN));

	}

	void removeOutEdge(EdbEdge e) {
		mOutEdges.remove(e);
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
		if (mId == null)
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

	/*
	 * public List<String> getInRelationWith(Vertex u) { List<String> relations
	 * = new ArrayList<String>(); for (EdbEdge e : mInEdges.get((EdbVertex) u))
	 * { relations.add(e.getLabel()); } return relations; }
	 * 
	 * public List<String> getOutRelationWith(Vertex u) { List<String> relations
	 * = new ArrayList<String>(); for (EdbEdge e : mOutEdges.get((EdbVertex) u))
	 * { relations.add(e.getLabel()); } return relations; }
	 */

}
