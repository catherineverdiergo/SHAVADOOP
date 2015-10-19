package com.tpt.shavadoop.master;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.util.CheckPing;

public class ResourceManager {
	
	List<String> availableHosts;

	List<String> buzzyHosts;
	
	private static ResourceManager instance=null;
	
	private static final Logger logger = Logger.getLogger(ResourceManager.class);
	
	public static List<String> getOnlineHosts() {
		if (instance==null) {
			instance = new ResourceManager();
			try {
				instance.availableHosts = CheckPing.getOnlineHosts(Configuration.getParameter("hosts.file"));
				instance.buzzyHosts = new ArrayList<String>();
			}
			catch (Exception e) {
				logger.error(e,e);
			}
		}
		return instance.availableHosts;
	}

	/**
	 * This method has been implemented for tests
	 * @param hostName
	 */
	public static void addOnlineHost(String hostName) {
		if (instance==null) {
			instance = new ResourceManager();
			try {
				instance.availableHosts = new ArrayList<String>();
				instance.buzzyHosts = new ArrayList<String>();
				if (CheckPing.checkHost(hostName)) {
					instance.availableHosts.add(hostName);
				}
			}
			catch (Exception e) {
				logger.error(e,e);
			}
		}
	}
	
	/**
	 * Get the first available host and reserve it for a task
	 * @return
	 */
	public static synchronized String reserveHost() {
		String result = null;
		if (instance.availableHosts.size()!=0) {
			result = instance.availableHosts.get(0);
			instance.availableHosts.remove(0);
			instance.buzzyHosts.add(result);
		}
		return result;
	}
	
	/**
	 * Change the status of the given host from buzzy to available
	 * @param hostName
	 */
	public static synchronized void releaseHost(String hostName) {
		if (instance.buzzyHosts.contains(hostName)) {
			instance.buzzyHosts.remove(hostName);
			instance.availableHosts.add(hostName);
		}
	}

}
