package org.eulerdb.kernel.storage;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.eulerdb.kernel.helper.ByteArrayHelper;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.util.FastOutputStream;

public class PropertyBinding extends TupleBinding {

	@Override
	public Object entryToObject(TupleInput in) {
		/*Map<String,Object> m = new Hashtable<String,Object>();
		TupleInput ti = (TupleInput) in;

		String str = ti.readString();
		
		if (str==null||str.length()==0) return m;

		str = str.substring(1, str.length() - 1);

		String[] part = str.split(",");
		//if(part.length<2) return m;
		String main = null;
		for (int i = 0; i < part.length; i++) {
			main = part[i];

			
			//if (i < part.length - 1) {
			//	main += "}";
			//}

			String[] subArray = main.split("=");
			
			if(subArray.length!=2) continue; 

			String key = subArray[0].substring(0, subArray[0].length() );
			Object e =subArray[1];
			
			m.put(key, e);
		}*/
		
		Map<String, Object> m = null;
		try {
			m = (Map<String, Object>) ByteArrayHelper.deserialize(in.getBufferBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return m;
	}

	@Override
	public void objectToEntry(Object map, TupleOutput out) {
		TupleOutput to = (TupleOutput) out;

		/*StringBuilder builder = new StringBuilder();
		builder.append("{");
		Iterator<java.util.Map.Entry<String, Object>> it = ((Hashtable<String,Object>) map).entrySet()
				.iterator();
		int size = ((Map) map).size();
		int index = 0;
		while (it.hasNext()) {
			java.util.Map.Entry<String, Object> e = it.next();

			try {
				builder.append(e.getKey()).append("=").append(
						e.getValue());
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			if (index < (size - 1)) {
				builder.append(",");
			}

			index++;
		}
		builder.append("}");

		to.writeString(builder.toString());*/
		
		try {
			to.write(ByteArrayHelper.serialize(map));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/*
	protected class Entry {

		public final static String KEY = "key";
		public final static String VALUE = "value";
		public final static String CONVERTER = "converter";

		private Converter converter = null;
		private String key = null;
		private Object value = null;

		public Entry(String key, Object value, Converter converter) {
			this.key = key;
			this.value = value;
			this.converter = converter;
		}

		public Entry(String key, Object value) {
			this.key = key;
			this.value = value;
		}

		public Converter getConverter() {
			return converter;
		}

		public void setConverter(Converter converter) {
			this.converter = converter;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public String toString() {
			return "key(" + key + "), value(" + value + "), convert("
					+ converter + ")";
		}
	}

	class EntryConverter implements Converter {

		public Object convertToObject(String data) throws Exception {

			String key = null;
			String value = null;
			Converter converter = null;

			// Ã³À½ÀÇ {, ¸¶Áö¸·ÀÇ } Á¦°Å
			String subData = data.substring(1, data.length() - 1);

			String[] mainArray = subData.split(", ");
			for (String main : mainArray) {
				String[] subArray = main.split("=");

				if (subArray[0].equals(Entry.KEY)) {
					key = subArray[1];
				} else if (subArray[0].equals(Entry.VALUE)) {
					value = subArray[1];
				} else if (subArray[0].equals(Entry.CONVERTER)) {
					converter = (Converter) Class.forName(subArray[1])
							.newInstance();
				} else {
					throw new Exception("unsupport keyword : " + subArray[0]);
				}
			}

			return new Entry(key, value, converter);

		}

		public String convertToString(Object obj) throws Exception {
			if (obj instanceof Entry) {
				Entry e = (Entry) obj;

				StringBuilder builder = new StringBuilder();
				
				if (e.getConverter() != null) {
					builder.append("{").append(Entry.KEY).append("=").append(e.getKey())
					.append(", ").append(Entry.VALUE).append("=").append(e.getConverter().convertToString(e.getValue()))
					.append(", ").append(Entry.CONVERTER).append("=").append(e.getConverter().getClass().getName())
					.append("}");
				} else {
					builder.append("{").append(Entry.KEY).append("=").append(e.getKey())
					.append(", ").append(Entry.VALUE).append("=").append(e.getValue())
					.append("}");
				}
				return builder.toString();
			}
			return null;
		}

	}*/

}
