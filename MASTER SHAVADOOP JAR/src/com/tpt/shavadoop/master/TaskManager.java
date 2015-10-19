package com.tpt.shavadoop.master;

import java.util.ArrayList;
import java.util.List;

import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.master.task.ITaskVoter;

public class TaskManager extends Thread {
	
	// Task manager is a singleton
	private static TaskManager instance;
	
	// Queues dedicated to mapper tasks
	private List<RemoteExecutor> mappersWaitingQueue;
	private List<RemoteExecutor> mappersRunningQueue;
	private List<RemoteExecutor> mappersFailedQueue;
	private List<RemoteExecutor> mappersCompletedQueue;
	
	// Queues dedicated to other tasks (sufflers or reducers)
	private List<RemoteExecutor> tskWaitingQueue;
	private List<RemoteExecutor> tskRunningQueue;
	private List<RemoteExecutor> tskFailedQueue;
	private List<RemoteExecutor> tskCompletedQueue;
	
	// Custom object dedicated to vote for the next candidate task (mappers excepted) 
	private ITaskVoter tskVoter;
	
	public static void initialization() {
		if (instance == null) {
			instance = new TaskManager();
			instance.mappersWaitingQueue = new ArrayList<RemoteExecutor>();
			instance.mappersRunningQueue = new ArrayList<RemoteExecutor>();
			instance.mappersFailedQueue = new ArrayList<RemoteExecutor>();
			instance.mappersCompletedQueue = new ArrayList<RemoteExecutor>();
			instance.tskWaitingQueue = new ArrayList<RemoteExecutor>();
			instance.tskRunningQueue = new ArrayList<RemoteExecutor>();
			instance.tskFailedQueue = new ArrayList<RemoteExecutor>();
			instance.tskCompletedQueue = new ArrayList<RemoteExecutor>();
			// !!! TODO !!! init TaskVoter from configuration file
		}
	}
	
	public static void addMapper(RemoteExecutor mapper) {
		if (instance == null) {
			initialization();
		}
		instance.mappersWaitingQueue.add(mapper);
	}
	
	/**
	 * Change status of a given task from waiting to running
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static synchronized void waiting2Running(String taskStyle, RemoteExecutor task) {
		String hostCandidate = ResourceManager.reserveHost();
		if (hostCandidate != null) {
			if ("mapper".equals(taskStyle)) {
				instance.mappersWaitingQueue.remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.mappersRunningQueue.add(task);
			}
			else {
				instance.tskWaitingQueue.remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.tskRunningQueue.add(task);
			}
		}
	}

	/**
	 * Change status of a given task from fail to running to completed or failed
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static synchronized void running2FinalState(String taskStyle, RemoteExecutor task) {
		if ("mapper".equals(taskStyle)) {
			instance.mappersRunningQueue.remove(task);
			if (instance.tskVoter.compileTaskResults("mapper", task)) {
				instance.mappersCompletedQueue.add(task);
			}
			else {
				instance.mappersFailedQueue.add(task);
			}
		}
		else {
			instance.tskRunningQueue.remove(task);
			if (instance.tskVoter.compileTaskResults(null, task)) {
				instance.tskCompletedQueue.add(task);
			}
			else {
				instance.tskFailedQueue.add(task);
			}
		}
		// Release host
		ResourceManager.releaseHost(task.getHostName());
		task.setHostName(null);
		task.close();
	}

	/**
	 * Change status of a given task from fail to running
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static synchronized void fail2Running(String taskStyle, RemoteExecutor task) {
		String hostCandidate = ResourceManager.reserveHost();
		if (hostCandidate != null) {
			if ("mapper".equals(taskStyle)) {
				instance.mappersFailedQueue.remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.mappersRunningQueue.add(task);
			}
			else {
				instance.tskFailedQueue.remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.tskRunningQueue.add(task);
			}
		}
	}
	
	/**
	 * Check if all mappers are completed or not
	 * @return
	 */
	private boolean allMappersCompleted() {
		return (instance.mappersWaitingQueue.size()+
				instance.mappersRunningQueue.size()+
				instance.mappersFailedQueue.size())==0;
	}
	
	private void manageRunningMappers() {
		for (RemoteExecutor re:instance.mappersRunningQueue) {
			if (!re.isAlive()) {
				running2FinalState("mapper", re);
			}
		}
	}

	@Override
	public void run() {
		// Manage mappers until all completed
		while (!allMappersCompleted()) {
			// manage next waiting
			if (instance.mappersWaitingQueue.size() > 0) {
				waiting2Running("mapper", instance.mappersWaitingQueue.get(0));
			}
			// manager next failed
			if (instance.mappersFailedQueue.size() > 0) {
				fail2Running("mapper", instance.mappersFailedQueue.get(0));
			}
			// manage running
			manageRunningMappers();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} // while mapper candidate exists
		
		
	}
	
}
