package Deprecated;

import java.util.Iterator;

import org.eulerdb.kernel.storage.EdbBaseCursor;

import com.tinkerpop.blueprints.Element;

public class IteratorFactory {
	public static <T extends Element> Iterable<T> getIterator(Object arg0) {
/*
		if (arg0 instanceof EdbBaseCursor) {
			return (new EdbIterableFromDatabase((EdbBaseCursor)arg0));
		}  else {
			return (new EdbIterableFromIterator<T>((Iterator<T>) arg0));
		}*/
		return null;
	}
	
}
