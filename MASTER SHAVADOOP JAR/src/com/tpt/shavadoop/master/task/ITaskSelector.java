package com.tpt.shavadoop.master.task;

import com.tpt.shavadoop.master.remote.RemoteExecutor;

public interface ITaskSelector {
	
	/**
	 * Get next task to perform
	 * @return
	 */
	public boolean hasNextCandidate();
	
	/**
	 * Compile remote executor results
	 * @param taskStyle could be "mapper" or null for other cases 
	 * @param task
	 * @return true if success and false if fails
	 */
	public boolean compileTaskResults(String taskStyle, RemoteExecutor task);
	
	/**
	 * If a specific action should be applied before shuffle, do it in this
	 * methos
	 */
	public void prepare4Shuffle();
	
	/**
	 * Last thing to do before job's end ?
	 */
	public void beforeJobEnd();

}
