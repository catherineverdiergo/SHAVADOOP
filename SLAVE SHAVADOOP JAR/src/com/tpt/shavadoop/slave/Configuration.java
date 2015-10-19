package com.tpt.shavadoop.slave;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.xml.DOMConfigurator;

public class Configuration {
	
	private static Configuration instance = null;
	
	private String confFileName;
	
	private Properties properties;
	
	private String locale = "en";
	
	private Properties messages;
	
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
		}
	}
	
	public static void initConfiguration(String fileName) {
		initConfiguration(fileName, "en");
	}

	public static String getParameter(String paramName) throws Exception {
		String result = null;
		if (instance == null) {
			throw new Exception("Configuration has not been initialized yet.\n"
					+ "Use initConfiguration static method first.");
		}
		else {
			result = instance.properties.getProperty(paramName);
		}
		return result;
	}

	public static String getMessage(String msgId) throws Exception {
		String result = null;
		if (instance == null) {
			throw new Exception("Configuration has not been initialized yet.\n"
					+ "Use initConfiguration static method first.");
		}
		else {
			result = instance.messages.getProperty(msgId);
		}
		return result;
	}
	
}
