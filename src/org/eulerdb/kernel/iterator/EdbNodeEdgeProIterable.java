package org.eulerdb.kernel.iterator;

import java.io.IOException;
import java.util.Iterator;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdbNodeEdgeProIterable implements Iterable<Edge> {

	private Cursor cursor;
	OperationStatus retVal;
	DatabaseEntry theKey;
	DatabaseEntry theData;


	public EdbNodeEdgeProIterable(Cursor cur,String key) {
		cursor = cur;
		try {
			theKey = new DatabaseEntry(ByteArrayHelper.serialize(key));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		theData = new DatabaseEntry();

	}

	@Override
	public Iterator<Edge> iterator() {
		
		retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);

		return new Iterator<Edge>() {

			@Override
			public boolean hasNext() {

				return (retVal == OperationStatus.SUCCESS);
			}

			@Override
			public Edge next() {
				Edge edge = null;
				try {
					edge =  (Edge) ByteArrayHelper.deserialize(theData
							.getData());
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				retVal = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
				return edge;

			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();

			}
		};
	}

}
