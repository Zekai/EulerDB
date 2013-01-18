package org.eulerdb.kernel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdge implements Edge,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1034618974276848252L;
	private EdbVertex mFromVertex; //head/in
	private EdbVertex mToVertex; //tail/out
	private String mRelation;
	private String mId;
	private Map<String, Object> mProps;

	public EdbEdge(Vertex n1, Vertex n2, Object id, String relation) {
		mFromVertex = (EdbVertex) n1;
		mToVertex = (EdbVertex) n2;
		mRelation = relation;
		mId = n1.getId()+"_"+relation+"_"+n2.getId();//FIXME id is not used here
		mProps = new  HashMap<String, Object>();
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
		mProps.put(arg0, arg1);
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

}
