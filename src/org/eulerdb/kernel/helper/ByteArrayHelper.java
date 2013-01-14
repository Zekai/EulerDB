package org.eulerdb.kernel.helper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ByteArrayHelper {
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
    
    public static byte[] concatByteArrays(byte[] array1, byte[] array2) {
		byte[] retArray = new byte[array1.length + array2.length];
		System.arraycopy(array1, 0, retArray, 0, array1.length);
		System.arraycopy(array2, 0, retArray, array1.length, array2.length);

		return retArray;

	}
    
    public static float[] appendToFloatArray(float[] array, float value) {
		float[] retArray = new float[array.length + 1];
		System.arraycopy(array, 0, retArray, 0, array.length);
		retArray[array.length] = value;
		return retArray;

	}
}
