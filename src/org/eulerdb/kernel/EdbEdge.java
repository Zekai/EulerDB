package org.eulerdb.kernel;

import java.io.Serializable;
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
	private Float mWeight;
	private String mRelation;
	private String mId;

	public EdbEdge(Vertex n1, Vertex n2, Object weight, String relation) {
		mFromVertex = (EdbVertex) n1;
		mToVertex = (EdbVertex) n2;
		mWeight = (Float) weight;
		mRelation = relation;
		mId = n1.getId()+"_"+relation+"_"+n2.getId();
	}

	@Override
	public Object getId() {
		return mId;
	}

	@Override
	public Object getProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object removeProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getLabel() {
		return mRelation;
	}

	@Override
	public Vertex getVertex(Direction arg0) throws IllegalArgumentException {
		if (arg0 == Direction.BOTH)
			throw new IllegalArgumentException("direction should be both");
		else if (arg0 == Direction.IN)
			return mFromVertex;
		else if (arg0 == Direction.OUT)
			return mToVertex;

		return null;
	}

	public Vertex getToVertex() {

		return mToVertex;
	}

}
