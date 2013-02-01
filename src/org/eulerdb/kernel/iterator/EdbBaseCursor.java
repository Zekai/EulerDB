package org.eulerdb.kernel.iterator;

import org.apache.log4j.Logger;

public interface EdbBaseCursor {
	Logger logger = Logger.getLogger(EdbBaseCursor.class.getCanonicalName());

	public boolean hasNext();
	public void close();
	public Object next();
	public Object getFirst();
	public void remove();
}
