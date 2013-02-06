package org.eulerdb.kernel;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eulerdb.kernel.commons.Common;
import org.eulerdb.kernel.iterator.EdbIterableFromIterator;
import org.eulerdb.kernel.storage.EdbManager;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class EdbVertex implements Vertex, Serializable {

	/**
	 * 
	 */
	protected transient Logger logger = Logger.getLogger(this.getClass().getCanonicalName());
	private static final long serialVersionUID = 5653068124792462590L;
	protected static final AtomicInteger uniqueId = new AtomicInteger(0);
	protected transient static EdbStorage mStorage = null;
	protected String mId;
	protected String mOwner;
	protected transient static List<String> sBlackList = Arrays
			.asList(new String[] { "id" });

	/**
	 * The Vertex constructor is invisible outside of the package. The only way to add Vertex is via graph's addVertex function.
	 * @param id
	 */
	EdbVertex(String owner,Object id) {
		mOwner = owner;
		mId = id == null ? String.valueOf(uniqueId.getAndIncrement()) : String
				.valueOf(id);
		logger.debug("EdbVertex constructor id "+ id+ " mid "+ mId);

		if (mStorage == null)
			mStorage = EdbManager.getDbInstance(mOwner);

		initSaving();
	}
	
	/**
	 * Re-Initialize transient variable after deserialization
	 * @param in
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(java.io.ObjectInputStream in)
		    throws IOException, ClassNotFoundException {
		    in.defaultReadObject();
		    
		    
		    if(logger==null)
		    	logger = Logger.getLogger(this.getClass().getCanonicalName());
		    
		    logger.debug("deserializing vertex");
		    
		    if(sBlackList==null)
		    	sBlackList = Arrays
					.asList(new String[] { "id" });
		    
		    if (mStorage == null)
		    	mStorage = EdbManager.getDbInstance(mOwner);
		}

	@Override
	public String getId() {

		return mId;
	}

	@Override
	public Object getProperty(String arg0) {
		logger.debug("getProperty of "+ arg0);
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId);

		return props.get(arg0);
	}

	@Override
	public Set<String> getPropertyKeys() {
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId);

		return props.keySet();
	}

	@Override
	public Object removeProperty(String arg0) {
		logger.debug("remove property "+ arg0);
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId);
		Object o = props.remove(arg0);
		mStorage.store(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId, props);
		return o;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		logger.debug("setProperty key: "+ arg0+" value: "+ arg1);
		if(arg0==null||arg0.equals("")) 
			throw ExceptionFactory.elementKeyCanNotBeEmpty();
		
		if (sBlackList.contains(arg0))
			throw ExceptionFactory.propertyKeyIdIsReserved(); 
		
		//mStorage.createSecondaryIfNeed(storeType.NODEPROPERTY,EdbTransactionalGraph.txs.get(),arg0);
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId);
		props.put(arg0, arg1);
		mStorage.store(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId, props);
	}

	
	private String getAdjacent(storeType type,String edgeId,Direction dir,String... arg1){
		int firstIndex = edgeId.indexOf(Common.SEPARATOR_VERTEX);
		int secondIndex = edgeId.lastIndexOf(Common.SEPARATOR_VERTEX);
		String fromId = edgeId.substring(0,firstIndex);
		String relation = edgeId.substring(firstIndex+1,secondIndex);
		String toId = edgeId.substring(secondIndex+1);
		
		logger.debug("caculate adjacent:  from: "+ fromId+" to : "+ toId+" relation "+relation );

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
		logger.debug("get edge iterable for vertex "+ mId+ " with property key: "+arg0+" value of "+ arg1);
		List<Edge> res = new ArrayList<Edge>();
		if (arg0 == Direction.IN) {
			List<String> inEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(),mId);
			for (String s : inEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, EdbTransactionalGraph.txs.get(), r));
				}
			}
		} else if (arg0 == Direction.OUT) {
			List<String> outEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(),mId);
			for (String s : outEdges) {
				String r = getAdjacent(storeType.EDGE,s, arg0, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, EdbTransactionalGraph.txs.get(), r));
				}
			}
		} else if (arg0 == Direction.BOTH) {
			List<String> inEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(),mId);
			for (String s : inEdges) {
				String r = getAdjacent(storeType.EDGE,s,  Direction.IN, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, EdbTransactionalGraph.txs.get(), r));
				}
			}
			List<String> outEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(),mId);
			for (String s : outEdges) {
				String r = getAdjacent(storeType.EDGE,s,  Direction.OUT, arg1);
				if (r != null)
				{
					res.add((Edge) mStorage.getObj(storeType.EDGE, EdbTransactionalGraph.txs.get(), r));
				}
			}
		}
		return res;//EdbEdgeIterableFromCollection(res);

	}

	@Override
	public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {
		logger.debug("get vertex iterable for vertex "+ mId+ " with direction : "+arg0+" para list of "+ arg1==null?"":arg1.toString());
		List<Vertex> res = new ArrayList<Vertex>();
		if (arg0 == Direction.IN) {
			List<String> inEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(),mId);
			for (String s : inEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.OUT, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), r));
				}
			}
		} else if (arg0 == Direction.OUT) {
			List<String> outEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(),mId);
			for (String s : outEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.IN, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), r));
				}
			}
		} else if (arg0 == Direction.BOTH) {
			List<String> inEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(),mId);
			for (String s : inEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.OUT, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), r));
				}
			}
			List<String> outEdges  = (List<String>) mStorage.getObj(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(),mId);
			for (String s : outEdges) {
				String r = getAdjacent(storeType.VERTEX,s, Direction.IN, arg1);
				if (r != null)
				{
					res.add((Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), r));
				}
			}
		}

		return new EdbIterableFromIterator(res.iterator());

	}

	@Override
	public Query query() {
		return new DefaultQuery(this);
	}
	
	private void initSaving() {
		logger.debug("initSaving");
		List<String> inEdge = new CopyOnWriteArrayList<String>();
		mStorage.store(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(), mId, inEdge);
		
		List<String> OutEdge = new CopyOnWriteArrayList<String>();
		mStorage.store(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(), mId, OutEdge);
		
		Map<String, Object> props = new Hashtable<String, Object>();
		mStorage.store(storeType.NODEPROPERTY, EdbTransactionalGraph.txs.get(), mId, props);//FIXME
		
	}

	/**
	 * see https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model for
	 * the in/out relation
	 * 
	 * @param e
	 */
	void addInEdge(EdbEdge e) {
		logger.debug("addInEdge "+e.getId());
		// mInRelationMap.put(e.getLabel(),
		// (EdbVertex)e.getVertex(Direction.IN));
		@SuppressWarnings("unchecked")
		List<String> inEdge = (CopyOnWriteArrayList<String>) mStorage.getObj(
				storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(), mId);
		if (inEdge.contains(e.getId())){
			logger.warn("edge has already exist "+e.getId());
			return;
		}
		inEdge.add((String) e.getId());
		mStorage.store(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(), mId, inEdge);
	}

	void addOutEdge(EdbEdge e) {
		logger.debug("addOutEdge "+e.getId());
		@SuppressWarnings("unchecked")
		List<String> ouEdge = (CopyOnWriteArrayList<String>) mStorage.getObj(
				storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(), mId);
		if (ouEdge.contains(e.getId()))
		{
			logger.warn("edge has already exist "+e.getId());
			return;
		}
		ouEdge.add((String) e.getId());
		mStorage.store(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(), mId, ouEdge);
	}

	void removeInEdge(EdbEdge e) {
		logger.debug("removeInEdge "+e.getId());
		List<String> ouEdge = (CopyOnWriteArrayList<String>) mStorage.getObj(
				storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(), mId);
		ouEdge.remove(e.getId());
		mStorage.store(storeType.VERTEX_IN, EdbTransactionalGraph.txs.get(), mId, ouEdge);
	}

	void removeOutEdge(EdbEdge e) {
		logger.debug("removeOutEdge "+e.getId());
		List<String> ouEdge = (CopyOnWriteArrayList<String>) mStorage.getObj(
				storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(), mId);
		ouEdge.remove(e.getId());
		mStorage.store(storeType.VERTEX_OUT, EdbTransactionalGraph.txs.get(), mId, ouEdge);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (mId == null ? 0 : mId.hashCode());
		logger.debug("hashCode for edge "+mId+" is "+result);
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
