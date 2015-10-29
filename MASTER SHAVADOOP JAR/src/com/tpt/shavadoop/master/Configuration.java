package com.tpt.shavadoop.master;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.xml.DOMConfigurator;

import com.tpt.shavadoop.master.split.ISplitter;

public class Configuration {
	
	// Configuration singleton
	private static Configuration instance = null;
	
	private static final String DEFAULT_CONF_FILE = "conf/master.properties";
	
	// Configuration file name (default is master.properties)
	private String confFileName;
	
	// UUID for working directory job
	private String workDir;
	
	// To hold all parameters in memory
	private Properties properties;
	
	// locale for application messages
	private String locale = "en";
	
	// to hold all application messages in memory
	private Properties messages;
	
	// used splitter
	private ISplitter splitter;
	
	/**
	 * Read and load configuration parameters
	 */
	private void loadProperties() {
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
	
	/**
	 * Load application messages
	 */
	private void loadMessages() {
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
	
	/**
	 * Configuration initialization
	 * @param fileName
	 * @param locale
	 */
	public static void initConfiguration(String fileName, String locale) {
		if (instance == null) {
			// initialize configuration singleton
			instance = new Configuration();
			// set and read configuration file name
			instance.confFileName = fileName;
			instance.loadProperties();
			// set locale
			instance.locale = locale;
			// read messages
			instance.loadMessages();
	    	// Set log4j configuration
	    	File logDir = new File("log");
	    	if (!logDir.exists())
	    		logDir.mkdir();
	    	DOMConfigurator.configure("conf/log4j.xml");
	    	try {
	    		// Create a specific directory for job
	    		String wkDirName = instance.properties.getProperty("tmp.dir")+UUID.randomUUID().toString();
	    		File wkDirFile = new File(wkDirName);
	    		if (!wkDirFile.exists()) {
	    			wkDirFile.mkdirs();
	    		}
	    		instance.workDir = wkDirName;
	    		// create the splitter
	    		String dataFileName = instance.properties.getProperty("data.file");
	    		instance.splitter = (ISplitter)Class.forName(instance.properties.getProperty("splitter.class")).getConstructor(String.class).newInstance(dataFileName);
	    	}
	    	catch (Exception e) {
	    		e.printStackTrace();
	    	}
		}
	}
	
	/**
	 * Configuration initialization with default locale
	 * @param fileName
	 */
	public static void initConfiguration(String fileName) {
		initConfiguration(fileName, "en");
	}

	/**
	 * Configuration initialization with default configuration file name and default locale
	 */
	public static void initConfiguration() {
		initConfiguration(DEFAULT_CONF_FILE, "en");
	}

	/**
	 * Get a given parameter value by parameter key
	 * @param paramName
	 * @return
	 */
	public static String getParameter(String paramName) {
		String result = null;
		if (instance == null) {
			initConfiguration();
		}
		result = instance.properties.getProperty(paramName);
		return result;
	}

	/**
	 * Get a given application message by message key
	 * @param msgId
	 * @return
	 */
	public static String getMessage(String msgId) {
		String result = null;
		if (instance == null) {
			initConfiguration();
		}
		result = instance.messages.getProperty(msgId);
		return result;
	}
	
	/**
	 * Get splitter instance
	 * @return
	 */
	public static ISplitter getSplitter() {
		if (instance == null) {
			initConfiguration();
		}
		return instance.splitter;
	}
	
	/**
	 * Close configuration
	 */
	public static void closeConfiguration() {
		instance = null;
	}

	/**
	 * Get working directory for current job execution
	 * @return
	 */
	public static String getWorkingDir() {
		if (instance == null) {
			initConfiguration();
		}
		return instance.workDir;
	}
}
