package org.eulerdb.kernel.storage;

import java.io.Serializable;

public interface Message extends Serializable, Cloneable {
	public String getKey();
	public void setKey(String key);
	public String getDate();
	public void setDate(String currentDate);
	
	public void objectToEntry(Object obj, Object out) throws Exception;
	public Message entryToObject(Object obj, Object in) throws Exception;
	public Object clone();
}