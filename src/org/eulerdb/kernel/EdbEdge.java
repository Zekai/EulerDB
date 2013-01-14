package org.eulerdb.kernel;

import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbEdge implements Edge {

	private EdbVertex mFromVertex;
	private EdbVertex mToVertex;
	private Float mWeight;
	private String mRelation;

	public EdbEdge(Vertex n1, Vertex n2, Object weight, String relation) {
		mFromVertex = (EdbVertex) n1;
		mToVertex = (EdbVertex) n2;
		mWeight = (Float) weight;
		mRelation = relation;
	}

	@Override
	public Object getId() {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
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

	public String getRelation() {

		return mRelation;
	}

	public Vertex getToVertex() {

		return mToVertex;
	}

}
