package com.tpt.shavadoop.master;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.xml.DOMConfigurator;

import com.tpt.shavadoop.master.split.ISplitter;

public class Configuration {
	
	private static Configuration instance = null;
	
	private String confFileName;
	
	private Properties properties;
	
	private String locale = "en";
	
	private Properties messages;
	
	private ISplitter splitter;
	
	private void readProperties() {
		try {
			FileInputStream fis = new FileInputStream(new File(confFileName));
			BufferedInputStream bis = new BufferedInputStream(fis);
			this.properties = new Properties();
			this.properties.load(bis);
			bis.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void readMessages() {
		try {
			FileInputStream fis = new FileInputStream(new File("conf/messages-"+this.locale+".properties"));
			BufferedInputStream bis = new BufferedInputStream(fis);
			this.messages = new Properties();
			this.messages.load(bis);
			bis.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initConfiguration(String fileName, String locale) {
		if (instance == null) {
			// initialize configuration singleton
			instance = new Configuration();
			// set and read configuration file name
			instance.confFileName = fileName;
			instance.readProperties();
			// set locale
			instance.locale = locale;
			// read messages
			instance.readMessages();
	    	// Set log4j configuration
	    	File logDir = new File("log");
	    	if (!logDir.exists())
	    		logDir.mkdir();
	    	DOMConfigurator.configure("conf/log4j.xml");
	    	try {
	    		String dataFileName = instance.properties.getProperty("data.file");
	    		instance.splitter = (ISplitter)Class.forName(instance.properties.getProperty("splitter.class")).getConstructor(String.class).newInstance(dataFileName);
	    	}
	    	catch (Exception e) {
	    		e.printStackTrace();
	    	}
		}
	}
	
	public static void initConfiguration(String fileName) {
		initConfiguration(fileName, "en");
	}

	public static String getParameter(String paramName) {
		String result = null;
		if (instance == null) {
			System.err.println("Configuration has not been initialized yet.\n"
					+ "Use initConfiguration static method first.");
			System.exit(-1);
		}
		else {
			result = instance.properties.getProperty(paramName);
		}
		return result;
	}

	public static String getMessage(String msgId) {
		String result = null;
		if (instance == null) {
			System.err.println("Configuration has not been initialized yet.\n"
					+ "Use initConfiguration static method first.");
			System.exit(-1);
		}
		else {
			result = instance.messages.getProperty(msgId);
		}
		return result;
	}
	
	public static ISplitter getSplitter() {
		if (instance == null) {
			return null;
		}
		else {
			return instance.splitter;
		}
	}
	
	public static void closeConfiguration() {
		instance = null;
	}

}
