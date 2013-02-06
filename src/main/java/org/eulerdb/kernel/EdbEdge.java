package org.eulerdb.kernel;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eulerdb.kernel.storage.EdbManager;
import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

public class EdbEdge implements Edge, Serializable {

	/**
	 * 
	 */
	protected transient Logger logger = Logger.getLogger(this.getClass().getCanonicalName());
	private static final long serialVersionUID = 1034618974276848252L;
	protected transient static List<String> sBlackList = Arrays.asList(new String[] { "id",
			"label" });
	protected String mFromVertex; // head/in
	protected String mToVertex; // tail/out
	protected String mRelation;
	protected String mId;
	protected String mOwner;
	protected transient static EdbStorage mStorage = null;

	/**
	 * The EdbEdge constructor is invisible outside of the package. The only way to add Edge is via graph's addEdge function.
	 * @param owner 
	 * @param id
	 */
	EdbEdge(String owner, Vertex n1, Vertex n2, Object id, String relation) {
		logger.debug("EdbEdge consturctor: from "+ n1.getId()+" to "+n2.getId()+" relation of "+ relation);
		mOwner = owner;
		mFromVertex = (String) n1.getId();
		mToVertex = (String) n2.getId();
		mRelation = relation;
		mId = n1.getId() + "_" + relation + "_" + n2.getId();// FIXME id is not
																// used here
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
		    
		    logger.debug("deserializing");
		    
		    if(sBlackList==null)
		    	sBlackList = Arrays.asList(new String[] { "id","label" });
		    
		    if (mStorage == null)
				mStorage = EdbManager.getDbInstance(mOwner);
		}

	@Override
	public Object getId() {
		return mId;
	}

	@Override
	public Object getProperty(String arg0) {
		logger.debug("getProperty of "+ arg0);
		@SuppressWarnings("unchecked")
		Map<String, Object> props = (Hashtable<String, Object>) mStorage
				.getObj(storeType.EDGEPROPERTY,
						EdbTransactionalGraph.txs.get(), mId);

		return props.get(arg0);
	}

	@Override
	public Set<String> getPropertyKeys() {
		@SuppressWarnings("unchecked")
		Map<String, Object> props = (Hashtable<String, Object>) mStorage
				.getObj(storeType.EDGEPROPERTY,
						EdbTransactionalGraph.txs.get(), mId);

		return props.keySet();
	}

	@Override
	public Object removeProperty(String arg0) {
		logger.debug("remove property "+ arg0);
		@SuppressWarnings("unchecked")
		Map<String, Object> props = (Hashtable<String, Object>) mStorage
				.getObj(storeType.EDGEPROPERTY,
						EdbTransactionalGraph.txs.get(), mId);
		Object o = props.remove(arg0);
		mStorage.store(storeType.EDGEPROPERTY, EdbTransactionalGraph.txs.get(),
				mId, props);
		return o;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		logger.debug("setProperty key: "+ arg0+" value: "+ arg1);
		
		if(arg0==null||arg0.equals("")) 
			throw ExceptionFactory.elementKeyCanNotBeEmpty();
		
		if (sBlackList.contains(arg0))
			throw new IllegalArgumentException(arg0
					+ " is not allowed to be used as property name");
		//mStorage.createSecondaryIfNeed(storeType.EDGEPROPERTY,EdbTransactionalGraph.txs.get(), arg0);
		@SuppressWarnings("unchecked")
		Map<String, Object> props = (Hashtable<String, Object>) mStorage
				.getObj(storeType.EDGEPROPERTY,
						EdbTransactionalGraph.txs.get(), mId);
		props.put(arg0, arg1);
		mStorage.store(storeType.EDGEPROPERTY, EdbTransactionalGraph.txs.get(),
				mId, props);

	}

	@Override
	public String getLabel() {
		return mRelation;
	}

	/**
	 * see the model for detail
	 */
	@Override
	public Vertex getVertex(Direction arg0) {
		logger.debug("getVertex of direction " +arg0);
		if (arg0 == Direction.IN)
			return (Vertex) mStorage.getObj(storeType.VERTEX,
					EdbTransactionalGraph.txs.get(), mToVertex);
		else if (arg0 == Direction.OUT)
			return (Vertex) mStorage.getObj(storeType.VERTEX,
					EdbTransactionalGraph.txs.get(), mFromVertex);
		else{
			
			logger.error("direction should not be "+ arg0);
			throw ExceptionFactory.bothIsNotSupported();//   IllegalArgumentException("direction should not be "+ arg0);
		}
	}

	private void initSaving() {
		logger.debug("initialize property for edge "+ mId);
		Map<String, Object> props = new Hashtable<String, Object>();
		mStorage.store(storeType.EDGEPROPERTY, EdbTransactionalGraph.txs.get(),
				mId, props);

	}

	public String getVertexId(Direction arg0) {
		logger.debug("getVertex of direction " +arg0);
		if (arg0 == Direction.IN)
			return mToVertex;
		else if (arg0 == Direction.OUT)
			return mFromVertex;
		else {
			logger.error("direction should not be "+ arg0);
			throw ExceptionFactory.bothIsNotSupported();
		}
	}

	public Vertex getToVertex() {
		logger.debug("getToVertex ");
		return (Vertex) mStorage.getObj(storeType.VERTEX,
				EdbTransactionalGraph.txs.get(), mFromVertex);
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
		EdbEdge other = (EdbEdge) obj;
		if (!mId.equals(other.getId()))
			return false;
		return true;
	}

}
