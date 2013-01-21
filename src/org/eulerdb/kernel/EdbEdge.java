package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	private EdbVertex mFromVertex; // head/in
	private EdbVertex mToVertex; // tail/out
	private String mRelation;
	private String mId;
	private Map<String, Object> mProps;
	protected transient static EdbStorage mStorage = null;

	public EdbEdge(Vertex n1, Vertex n2, Object id, String relation) {
		mFromVertex = (EdbVertex) n1;
		mToVertex = (EdbVertex) n2;
		mRelation = relation;
		mId = n1.getId() + "_" + relation + "_" + n2.getId();// FIXME id is not
																// used here
		if(mStorage==null) mStorage = EdbStorage.getInstance("",true);
		mProps = new HashMap<String, Object>();
	}

	@Override
	public Object getId() {
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
		if (sBlackList.contains(arg0))
			throw new IllegalArgumentException(arg0
					+ " is not allowed to be used as property name");
		mProps.put(arg0, arg1);
		mStorage.store(storeType.EDGE, this);//FIXME this code make it nontransactional

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
			return mToVertex;
		else if (arg0 == Direction.OUT)
			return mFromVertex;

		return null;
	}

	public Vertex getToVertex() {

		return mToVertex;
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
		EdbEdge other = (EdbEdge) obj;
		if (!mId.equals(other.getId()))
			return false;
		return true;
	}
	
	

}
