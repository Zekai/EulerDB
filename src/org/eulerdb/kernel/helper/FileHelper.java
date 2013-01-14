package org.eulerdb.kernel.helper;

import java.io.File;
import java.io.IOException;

public class FileHelper {

	public static boolean deleteDir(String dir) { 
		File f = new File(dir);
		return deleteDir(f);
	}
	
	public static boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }

	    return dir.delete();
	}
	
	public static void delFile(String wDir, String file) {
		File f = new File(wDir+"/"+file);
		if (f.exists()) f.delete();
	}
	
}
