package org.eulerdb.kernel.storage;

public interface Converter {
	public String convertToString(Object obj) throws Exception;
	public Object convertToObject(String data) throws Exception ;
}
