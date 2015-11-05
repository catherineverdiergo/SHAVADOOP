package com.tpt.shavadoop.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.monitor.DashBoardServer;
import com.tpt.shavadoop.master.remote.AgregatorRemoteExecutor;
import com.tpt.shavadoop.master.remote.ReducerRemoteExecutor;
import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.master.remote.ShuffleRemoteExecutor;
import com.tpt.shavadoop.master.task.ITaskSelector;
import com.tpt.shavadoop.util.FileUtils;

public class TaskManager extends Thread {
	
	// Task manager is a singleton
	private static TaskManager instance;
	
	private static final Logger logger = Logger.getLogger(TaskManager.class);
	
	public static final int WAITING_QUEUE   = 0; 
	public static final int RUNNING_QUEUE   = 1; 
	public static final int FAILED_QUEUE    = 2; 
	public static final int COMPLETED_QUEUE = 3;
	
	public static final String[] QUEUE_LABELS = {
			"WAITING_QUEUE",
			"RUNNING_QUEUE",
			"FAILED_QUEUE",
			"COMPLETED_QUEUE"
	};
	
	// Queues dedicated to mapper tasks
	private Map<Integer,List<RemoteExecutor>> mapperQueues;
	
	// Queues dedicated to other tasks (suffle, reduce or agregate)
	private Map<Integer,List<RemoteExecutor>> taskQueues;
	
	// Custom object dedicated to vote for the next candidate task (mappers excepted) 
	private ITaskSelector tskSelector;
	
	// Web server to monitor system
	private DashBoardServer monitorHttpServer;
	
	public static void initialization() {
		if (instance == null) {
			instance = new TaskManager();
			instance.mapperQueues = new HashMap<Integer,List<RemoteExecutor>>();
			instance.taskQueues = new HashMap<Integer,List<RemoteExecutor>>();
			for (int i = WAITING_QUEUE; i<= COMPLETED_QUEUE; i++) {
				instance.mapperQueues.put(i, new ArrayList<RemoteExecutor>());
				instance.taskQueues.put(i, new ArrayList<RemoteExecutor>());
			}
			// read the class name ok the task selector to use from configuration
			String tskSelectorClassName = Configuration.getParameter("task.selector");
			// create the task selector
			try {
				instance.tskSelector = (ITaskSelector)Class.forName(tskSelectorClassName).newInstance();
			} catch (Exception e) {
				logger.error(e,e);
			}
			// Create and start monitor http server
			instance.monitorHttpServer = new DashBoardServer();
			instance.monitorHttpServer.start();
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
		instance.mapperQueues.get(WAITING_QUEUE).add(mapper);
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
		instance.taskQueues.get(WAITING_QUEUE).add(task);
	}
	
	/**
	 * Change status of a given task from waiting to running
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static void waiting2Running(String taskStyle, RemoteExecutor task) {
		String hostCandidate = ResourceManager.reserveHost();
		if (hostCandidate != null) {
			if ("mapper".equals(taskStyle)) {
				instance.mapperQueues.get(WAITING_QUEUE).remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.mapperQueues.get(RUNNING_QUEUE).add(task);
			}
			else {
				instance.taskQueues.get(WAITING_QUEUE).remove(task);
				task.setHostName(hostCandidate);
				task.start();
				instance.taskQueues.get(RUNNING_QUEUE).add(task);
			}
		}
	}

	/**
	 * Change status of a given task from fail to running to completed or failed
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static void running2FinalState(String taskStyle, RemoteExecutor task) {
		if ("mapper".equals(taskStyle)) {
			instance.mapperQueues.get(RUNNING_QUEUE).remove(task);
			if (instance.tskSelector.compileTaskResults("mapper", task)) {
				instance.mapperQueues.get(COMPLETED_QUEUE).add(task);
			}
			else {
				instance.mapperQueues.get(FAILED_QUEUE).add(task);
			}
		}
		else {
			instance.taskQueues.get(RUNNING_QUEUE).remove(task);
			if (instance.tskSelector.compileTaskResults(instance.getTaskStyle(task), task)) {
				instance.taskQueues.get(COMPLETED_QUEUE).add(task);
			}
			else {
				instance.taskQueues.get(FAILED_QUEUE).add(task);
			}
		}
		// Release host
		ResourceManager.releaseHost(task.getHostName());
//		task.setHostName(null);
		task.close();
	}

	/**
	 * Change status of a given task from fail to running
	 * @param taskStyle could be "mapper" or "other"
	 * @param task
	 */
	public static void fail2Running(String taskStyle, RemoteExecutor task) {
		if ("mapper".equals(taskStyle)) {
			instance.mapperQueues.get(FAILED_QUEUE).remove(task);
			RemoteExecutor replayMapper = new RemoteExecutor(task.getCommand());
			instance.mapperQueues.get(WAITING_QUEUE).add(replayMapper);
		}
		else {
			instance.taskQueues.get(FAILED_QUEUE).remove(task);
			if (task instanceof ShuffleRemoteExecutor) {
				ShuffleRemoteExecutor replayTask = new ShuffleRemoteExecutor(task.getCommand());
				replayTask.setWordKey(((ShuffleRemoteExecutor)task).getWordKey());
				instance.taskQueues.get(WAITING_QUEUE).add(replayTask);
			}
			else if (task instanceof ReducerRemoteExecutor) {
				ReducerRemoteExecutor replayTask = new ReducerRemoteExecutor(task.getCommand());
				instance.taskQueues.get(WAITING_QUEUE).add(replayTask);
			}
			else if (task instanceof AgregatorRemoteExecutor) {
				AgregatorRemoteExecutor replayTask = new AgregatorRemoteExecutor(task.getCommand());
				instance.taskQueues.get(WAITING_QUEUE).add(replayTask);
			}
		}
	}
	
	/**
	 * Check if all mappers are completed or not
	 * @return
	 */
	private boolean allMappersCompleted() {
		return (instance.mapperQueues.get(WAITING_QUEUE).size()+
				instance.mapperQueues.get(RUNNING_QUEUE).size()+
				instance.mapperQueues.get(FAILED_QUEUE).size())==0;
	}
	
	private void manageRunningMappers() {
		RemoteExecutor re = null;
		for (RemoteExecutor mapper:instance.mapperQueues.get(RUNNING_QUEUE)) {
			if (!mapper.isAlive()) {
				re = mapper;
				break;
			}
		}
		if (re != null) {
			running2FinalState("mapper", re);
		}
	}

	/**
	 * Check if all tasks are completed or not
	 * @return
	 */
	private boolean allTasksCompleted() {
		return (instance.taskQueues.get(WAITING_QUEUE).size()+
				instance.taskQueues.get(RUNNING_QUEUE).size()+
				instance.taskQueues.get(FAILED_QUEUE).size())==0;
	}
	
	private void manageRunningTasks() {
		RemoteExecutor re = null;
		for (RemoteExecutor mapper:instance.taskQueues.get(RUNNING_QUEUE)) {
			if (!mapper.isAlive()) {
				re = mapper;
				break;
			}
		}
		if (re != null) {
			running2FinalState(getTaskStyle(re), re);
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
			if (instance.mapperQueues.get(WAITING_QUEUE).size() > 0) {
				waiting2Running("mapper", instance.mapperQueues.get(WAITING_QUEUE).get(0));
			}
			// manager next failed
			if (instance.mapperQueues.get(FAILED_QUEUE).size() > 0) {
				fail2Running("mapper", instance.mapperQueues.get(FAILED_QUEUE).get(0));
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
			if (instance.taskQueues.get(WAITING_QUEUE).size() > 0) {
				RemoteExecutor task = instance.taskQueues.get(WAITING_QUEUE).get(0);
				waiting2Running(getTaskStyle(task), task);
			}
			// manager next failed
			if (instance.taskQueues.get(FAILED_QUEUE).size() > 0) {
				RemoteExecutor task = instance.taskQueues.get(FAILED_QUEUE).get(0);
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
		for (RemoteExecutor re:instance.mapperQueues.get(RUNNING_QUEUE)) {
			if (re.isAlive()) {
				re = null;
			}
		}
		instance.mapperQueues.get(RUNNING_QUEUE).clear();
		for (RemoteExecutor re:instance.taskQueues.get(RUNNING_QUEUE)) {
			if (re.isAlive()) {
				re = null;
			}
		}
		instance.taskQueues.get(RUNNING_QUEUE).clear();
		// stop monitor http server
		instance.monitorHttpServer = null;
	}
	
	/**
	 * generate shavadoop global status as json properties 
	 * @return
	 */
	public synchronized static String getGlobalStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("\"shavadoop status\" :");
		if (TaskManager.isRunning()) {
			sb.append("\"job in progess...\"");
		}
		else {
			sb.append("\"job completed\"");
		}
		sb.append(",\"working dir\" :\""+new File(Configuration.getWorkingDir()).getAbsolutePath()+"\"");
		return sb.toString();
	}
	
	/**
	 * Return mapper queue status as json string
	 * @return
	 */
	public synchronized static String getStatusMappers() {
		StringBuffer sb = new StringBuffer();
		sb.append("\"mapper queues\" :").append("{");
		for (int i = WAITING_QUEUE; i <= COMPLETED_QUEUE; i++) {
			sb.append("\"").append(QUEUE_LABELS[i]).append("\":");
			sb.append("{").append("\"nb entries\" :").append(instance.mapperQueues.get(i).size()).append(",");
			sb.append("\"entries\" :[");
			for (int j = 0; j < instance.mapperQueues.get(i).size(); j++) {
				RemoteExecutor entry = instance.mapperQueues.get(i).get(j);
				if (i == WAITING_QUEUE) {
					sb.append("\"").append(entry.getCommand().replace("\"", "\\\"")).append("\"");
				}
				else {
					sb.append("{\"command\":").append("\"").append(entry.getCommand().replace("\"", "\\\"")).append("\"")
					.append(",\"host\":\"").append(entry.getHostName()).append("\"}");					
				}
				if (j < instance.mapperQueues.get(i).size()-1) {
					sb.append(",");
				}
			}
			sb.append("]");
			sb.append("}");
			if (i < COMPLETED_QUEUE) {
				sb.append(",");
			}
		}
		sb.append("}");
		return sb.toString();
	}

	/**
	 * Return task queue status as json string
	 * @return
	 */
	public synchronized static String getStatusTasks() {
		StringBuffer sb = new StringBuffer();
		sb.append("\"task queues\" :").append("{");
		for (int i = WAITING_QUEUE; i <= COMPLETED_QUEUE; i++) {
			sb.append("\"").append(QUEUE_LABELS[i]).append("\":");
			sb.append("{").append("\"nb entries\" :").append(instance.taskQueues.get(i).size()).append(",");
			sb.append("\"entries\" :[");
			for (int j = 0; j<instance.taskQueues.get(i).size(); j++) {
				RemoteExecutor entry = instance.taskQueues.get(i).get(j);
				if (i == WAITING_QUEUE) {
					sb.append("\"").append(entry.getCommand().replace("\"", "\\\"")).append("\"");
				}
				else {
					sb.append("{\"command\":").append("\"").append(entry.getCommand().replace("\"", "\\\"")).append("\"")
					.append(",\"host\":\"").append(entry.getHostName()).append("\"}");					
				}
				if (j < instance.taskQueues.get(i).size()-1) {
					sb.append(",");
				}
			}
			sb.append("]");
			sb.append("}");
			if (i < COMPLETED_QUEUE) {
				sb.append(",");
			}
		}
		sb.append("}");
		return sb.toString();
	}

	/**
	 * Returns queues status as json string
	 * @return
	 */
	public synchronized static String getStatus() {
		StringBuffer sb = new StringBuffer();
		sb.append("{").append(getGlobalStatus()).append(",").append(getStatusMappers()).append(",").append(getStatusTasks()).append("}");
		return sb.toString();
	}
	
	/**
	 * Show result file
	 * @return
	 */
	public static String showResult() {
		StringBuffer sb = new StringBuffer();
		String resFileName = FileUtils.addBackspaces(Configuration.getWorkingDir()+"/Result");
		BufferedReader br = FileUtils.openFile4Read(resFileName);
		String line;
		try {
			line = br.readLine();
			while (line != null) {
				line = br.readLine();
				sb.append(line).append("<br>");
			}
		} catch (IOException e) {
			logger.error(e,e);
		}
		return sb.toString();
	}
	
}
