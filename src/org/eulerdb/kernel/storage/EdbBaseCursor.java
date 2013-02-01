package org.eulerdb.kernel.storage;

public interface EdbBaseCursor {

	public boolean hasNext();
	public void close();
	public Object next();
	public Object getFirst();
	public void remove();
}
