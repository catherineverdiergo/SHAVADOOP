package com.tpt.shavadoop.master;

import java.util.ArrayList;
import java.util.List;

import com.tpt.shavadoop.master.remote.RemoteExecutor;

public class Master {
	
	/**
	 * Test if a set of tasks have been performed
	 * @param l : list of threads
	 * @return
	 */
	public static boolean isFinished(List<RemoteExecutor> l) {
		boolean result = true;
		for (RemoteExecutor tsk:l) {
			if (tsk.isAlive()) {
				result = false;
				break;
			}
		}
		return result;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration.initConfiguration("conf/master.properties");
		List<String> hosts = ResourceManager.getOnlineHosts();
		List<RemoteExecutor> res = new ArrayList<RemoteExecutor>();
		// Create a thread for each available host
		for (String host:hosts) {
			RemoteExecutor re = new RemoteExecutor("./waiter.jar");
			re.setHostName(host);
			res.add(re);
		}
		// Stat all threads
		for (RemoteExecutor tsk:res) {
			tsk.start();
		}
		// wait for all thread finished
		while (!isFinished(res));
		// All finished
		System.out.println("All finished");
	}

}
