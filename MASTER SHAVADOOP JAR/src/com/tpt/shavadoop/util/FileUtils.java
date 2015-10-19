package com.tpt.shavadoop.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtils {
	
	public static BufferedReader openFile4Read(String fileName) {
	    BufferedReader lecteurAvecBuffer=null;
	    
	    try {
	    	lecteurAvecBuffer = new BufferedReader(new FileReader(fileName));
	    }
	    catch(FileNotFoundException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur d'ouverture");
	    }
    	return lecteurAvecBuffer;
	}
	
	public static BufferedWriter openFile4Write(String fileName) {
	    BufferedWriter ecrivainAvecBuffer=null;
	    
	    try {
			File file = new File(fileName);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			else {
				file.delete();
			}

	    	ecrivainAvecBuffer = new BufferedWriter(new FileWriter(fileName));
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur d'ouverture");
	    }
    	return ecrivainAvecBuffer;
	}
	
	public static BufferedOutputStream openBinFile4Write(String fileName) {
	    BufferedOutputStream ecrivainAvecBuffer=null;
	    
	    try {
			File file = new File(fileName);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			else {
				file.delete();
			}

	    	ecrivainAvecBuffer = new BufferedOutputStream(new FileOutputStream(fileName));
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur d'ouverture");
	    }
    	return ecrivainAvecBuffer;
	}
	
	public static void close(BufferedReader fileHandler) {
	    try {
	    	fileHandler.close();
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur de fermeture");
	    }
	}
	
	public static void close(BufferedWriter fileHandler) {
	    try {
	    	fileHandler.close();
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur de fermeture");
	    }
	}
	
	public static void close(BufferedOutputStream fileHandler) {
	    try {
	    	fileHandler.close();
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur de fermeture");
	    }
	}
	
	public static void write(BufferedWriter fileHandler, String host) {
	    try {
	    	fileHandler.write(host+System.lineSeparator());
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur de fermeture");
	    }
	}
	
	public static String getNextLine(BufferedReader fileHandler) {
		String result = null;
	    try {
	    	result = fileHandler.readLine();
	    }
	    catch(IOException exc) {
	    	exc.printStackTrace();
	    	System.out.println("Erreur de fermeture");
	    }
		return result;
	}
	
	public static void delete(File f) throws IOException {
		  if (f.isDirectory()) {
		    for (File c : f.listFiles())
		      delete(c);
		  }
		  if (!f.delete())
		    throw new FileNotFoundException("Failed to delete file: " + f);
	}
}
