package org.eulerdb.kernel.iterator;

import java.util.Collection;
import java.util.Iterator;
import org.eulerdb.kernel.EdbVertex;
import org.eulerdb.kernel.storage.EdbCursor;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Element;

public class IteratorFactory {
	public static <T extends Element> Iterable<T> getIterator(Object arg0) {

		if (arg0 instanceof EdbCursor) {
			return (new EdbIterableFromDatabase<T>((EdbCursor)arg0));
		} else if (arg0 instanceof Collection){
			return (new EdbIterableFromCollection<T>((Collection<T>) arg0));
		} else {
			return (new EdbIterableFromIterator<T>((Iterator<T>) arg0));
		}
	}
}
