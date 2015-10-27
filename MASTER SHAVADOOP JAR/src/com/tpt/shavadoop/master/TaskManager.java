package com.tpt.shavadoop.master;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.remote.AgregatorRemoteExecutor;
import com.tpt.shavadoop.master.remote.ReducerRemoteExecutor;
import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.master.remote.ShuffleRemoteExecutor;
import com.tpt.shavadoop.master.task.ITaskSelector;

public class TaskManager extends Thread {
	
	// Task manager is a singleton
	private static TaskManager instance;
	
	private static final Logger logger = Logger.getLogger(TaskManager.class);
	
	// Queues dedicated to mapper tasks
	private List<RemoteExecutor> mappersWaitingQueue;
	private List<RemoteExecutor> mappersRunningQueue;
	private List<RemoteExecutor> mappersFailedQueue;
	private List<RemoteExecutor> mappersCompletedQueue;
	
	// Queues dedicated to other tasks (suffle, reduce or agregate)
	private List<RemoteExecutor> tskWaitingQueue;
	private List<RemoteExecutor> tskRunningQueue;
	private List<RemoteExecutor> tskFailedQueue;
	private List<RemoteExecutor> tskCompletedQueue;
	
	// Custom object dedicated to vote for the next candidate task (mappers excepted) 
	private ITaskSelector tskSelector;
	
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
			// read the class name ok the task selector to use from configuration
			String tskSelectorClassName = Configuration.getParameter("task.selector");
			// create the task selector
			try {
				instance.tskSelector = (ITaskSelector)Class.forName(tskSelectorClassName).newInstance();
			} catch (Exception e) {
				logger.error(e,e);
			}
		}
	}
	
	/**
	 * Add a mapper to mapper queue
	 * @param mapper
	 */
	public static void addMapper(RemoteExecutor mapper) {
		if (instance == null) {
			initialization();
		}
		logger.info("Adding mapper to queue - command is \""+mapper.getCommand()+"\"");
		instance.mappersWaitingQueue.add(mapper);
	}
	
	/**
	 * Add a task to task queue
	 * @param task
	 */
	public static void addTask(RemoteExecutor task) {
		if (instance == null) {
			initialization();
		}
		logger.info("Adding task to queue - command is \""+task.getCommand()+"\"");
		instance.tskWaitingQueue.add(task);
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
			if (instance.tskSelector.compileTaskResults("mapper", task)) {
				instance.mappersCompletedQueue.add(task);
			}
			else {
				instance.mappersFailedQueue.add(task);
			}
		}
		else {
			instance.tskRunningQueue.remove(task);
			if (instance.tskSelector.compileTaskResults(null, task)) {
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

	/**
	 * Check if all tasks are completed or not
	 * @return
	 */
	private boolean allTasksCompleted() {
		return (instance.tskWaitingQueue.size()+
				instance.tskRunningQueue.size()+
				instance.tskFailedQueue.size())==0;
	}
	
	private void manageRunningTasks() {
		for (RemoteExecutor re:instance.tskRunningQueue) {
			if (!re.isAlive()) {
				running2FinalState(getTaskStyle(re), re);
			}
		}
	}

	/**
	 * Identify style of a no mapper task (could be shuffle or reducer or agregator)
	 * @param task
	 * @return
	 */
	private String getTaskStyle(RemoteExecutor task) {
		String result = null;
		if (task instanceof ShuffleRemoteExecutor) {
			result = "shuffle";
		}
		else if (task instanceof ReducerRemoteExecutor) {
			result = "reducer";
		}
		else if (task instanceof AgregatorRemoteExecutor) {
			result = "agregator";
		}
		return result;
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
		
		// if a specific action should be done before shuffle do it in
		// the prepare4Shuffle implementation 
		tskSelector.prepare4Shuffle();
		
		// Manage tasks until all completed
		while (!allTasksCompleted() && tskSelector.hasNextCandidate()) {
			// manage next waiting
			if (instance.tskWaitingQueue.size() > 0) {
				RemoteExecutor task = instance.tskWaitingQueue.get(0);
				waiting2Running(getTaskStyle(task), task);
			}
			// manager next failed
			if (instance.mappersFailedQueue.size() > 0) {
				RemoteExecutor task = instance.tskFailedQueue.get(0);
				fail2Running(getTaskStyle(task), task);
			}
			// manage running
			manageRunningTasks();
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// When all is completed
			if (allTasksCompleted()) {
				tskSelector.beforeJobEnd();
			}
		} // while task candidate exists
		
	}
	
	/**
	 * Check if TaskManager singleton is running or not
	 */
	public static boolean isRunning() {
		return instance.isAlive();
	}
	
	/**
	 * Starting TaskManager singleton thread
	 */
	public static void applyStarter() {
		if (!instance.isAlive()) {
			instance.start();
		}
	}
	
	/**
	 * Shutdown task manager
	 */
	public static void shutdown() {
		// stop all running tasks
		for (RemoteExecutor re:instance.mappersRunningQueue) {
			if (re.isAlive()) {
				re = null;
			}
		}
		instance.mappersRunningQueue.clear();
		for (RemoteExecutor re:instance.tskRunningQueue) {
			if (re.isAlive()) {
				re = null;
			}
		}
		instance.tskRunningQueue.clear();
	}
	
}
