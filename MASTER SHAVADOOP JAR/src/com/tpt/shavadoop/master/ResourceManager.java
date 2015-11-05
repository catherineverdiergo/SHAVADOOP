package com.tpt.shavadoop.master;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.util.CheckPing;

public class ResourceManager {
	
	public static final int MAX_TASK_PER_HOST=4;
	
	private Map<String,Integer> availableHosts;
	
	private int nbTasksPerHost = ResourceManager.MAX_TASK_PER_HOST;

//	List<String> buzzyHosts;
	
	private static ResourceManager instance=null;
	
	private static final Logger logger = Logger.getLogger(ResourceManager.class);
	
	public static Map<String,Integer> getOnlineHosts() {
		if (instance==null) {
			instance = new ResourceManager();
			try {
				List<String> onlineHosts = CheckPing.getOnlineHosts(Configuration.getParameter("hosts.file")); 
				instance.availableHosts = new HashMap<String, Integer>();
				for (String host:onlineHosts) {
					instance.availableHosts.put(host, 1);
				}
//				instance.buzzyHosts = new ArrayList<String>();
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
				instance.availableHosts = new HashMap<String, Integer>();
//				instance.buzzyHosts = new ArrayList<String>();
				if (CheckPing.checkHost(hostName)) {
					instance.availableHosts.put(hostName,1);
				}
				if (Configuration.getParameter("remote.nbtasks") != null) {
					try {
						instance.nbTasksPerHost = Integer.parseInt(Configuration.getParameter("remote.nbtasks"));
					}
					catch(Exception e) {
						instance.nbTasksPerHost = MAX_TASK_PER_HOST;
					}
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
		if (!instance.availableHosts.isEmpty()) {
			Iterator<String> itHost = instance.availableHosts.keySet().iterator();
			while (itHost.hasNext()) {
				String host = itHost.next();
				int counterHost = instance.availableHosts.get(host);
				if (counterHost < instance.nbTasksPerHost) {
					result = host;
					instance.availableHosts.put(host, counterHost++);
					break;
				}
			}
//			result = instance.availableHosts.get(0);
//			instance.availableHosts.remove(0);
//			instance.buzzyHosts.add(result);
		}
		return result;
	}
	
	/**
	 * Change the status of the given host from buzzy to available
	 * @param hostName
	 */
	public static synchronized void releaseHost(String hostName) {
//		if (instance.buzzyHosts.contains(hostName)) {
//			instance.buzzyHosts.remove(hostName);
//			instance.availableHosts.add(hostName);
//		}
		int counterHost = instance.availableHosts.get(hostName);
		instance.availableHosts.put(hostName, counterHost--);
	}

}
