package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eulerdb.kernel.storage.EdbStorage;
import org.eulerdb.kernel.storage.EdbStorage.storeType;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdge implements Edge, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1034618974276848252L;
	private static List<String> sBlackList = Arrays.asList(new String[] { "id",
			"label" });
	private String mFromVertex; // head/in
	private String mToVertex; // tail/out
	private String mRelation;
	private String mId;
	protected transient static EdbStorage mStorage = null;

	public EdbEdge(Vertex n1, Vertex n2, Object id, String relation) {
		mFromVertex = (String) n1.getId();
		mToVertex =	(String) n2.getId();
		mRelation = relation;
		mId = n1.getId() + "_" + relation + "_" + n2.getId();// FIXME id is not
																// used here
		if(mStorage==null) mStorage = EdbStorage.getInstance();
		initSaving();
	}

	@Override
	public Object getId() {
		return mId;
	}

	@Override
	public Object getProperty(String arg0) {
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId);

		return props.get(arg0);
	}

	@Override
	public Set<String> getPropertyKeys() {
		@SuppressWarnings("unchecked")
		Map<String,Object> props = (Hashtable<String,Object>) mStorage.getObj(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId);

		return props.keySet();
	}

	@Override
	public Object removeProperty(String arg0) {
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId);
		Object o = props.remove(arg0);
		mStorage.store(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId, props);
		return o;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		if (sBlackList.contains(arg0))
			throw new IllegalArgumentException(arg0
					+ " is not allowed to be used as property name");
		@SuppressWarnings("unchecked")
		Map<String,Object> props =  (Hashtable<String,Object>) mStorage.getObj(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId);
		props.put(arg0, arg1);
		mStorage.store(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId, props);

	}

	@Override
	public String getLabel() {
		return mRelation;
	}

	/**
	 * see the model for detail
	 */
	@Override
	public Vertex getVertex(Direction arg0) throws IllegalArgumentException {
		if (arg0 == Direction.BOTH)
			throw new IllegalArgumentException("direction should be both");
		else if (arg0 == Direction.IN)
			return (Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), mToVertex);
		else if (arg0 == Direction.OUT)
			return (Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), mFromVertex);

		return null;
	}
	
	private void initSaving() {
		
		Map<String, Object> props = new Hashtable<String, Object>();
		mStorage.store(storeType.PROPERTY, EdbTransactionalGraph.txs.get(), mId, props);
		
	}
	
	public String getVertexId(Direction arg0) {
		if (arg0 == Direction.BOTH)
			throw new IllegalArgumentException("direction should be both");
		else if (arg0 == Direction.IN)
			return mToVertex;
		else if (arg0 == Direction.OUT)
			return mFromVertex;

		return null;
	}

	public Vertex getToVertex() {

		return (Vertex) mStorage.getObj(storeType.VERTEX, EdbTransactionalGraph.txs.get(), mFromVertex);
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
		if(mId==null)
			return false;
		if (!(obj.getClass() == getClass()))
			return false;
		EdbEdge other = (EdbEdge) obj;
		if (!mId.equals(other.getId()))
			return false;
		return true;
	}

}
