package org.eulerdb.kernel.iterator;

import java.util.Iterator;
import java.util.ListIterator;

import org.eulerdb.kernel.storage.EdbCursor;
import org.eulerdb.kernel.storage.TestEntityBinding;

import com.sleepycat.collections.StoredList;
import com.sleepycat.je.Database;
import com.tinkerpop.blueprints.Vertex;

public class EdbVertexIterableFromDatabase2  extends EdbVertexIterable{

	private Database mdb;
	public EdbVertexIterableFromDatabase2(Database db) {
		mdb = db;
	}
	
	@Override
	public Iterator<Vertex> iterator() {
		 StoredList sl = new StoredList(mdb,new TestEntityBinding(true),true);
		 final ListIterator it = sl.listIterator();
		 return sl.listIterator();
	}

}
