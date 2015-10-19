package com.tpt.shavadoop.master.task;

import com.tpt.shavadoop.master.remote.RemoteExecutor;

public interface ITaskVoter {
	
	/**
	 * Get next task to perform
	 * @return
	 */
	public RemoteExecutor getNextCandidate();
	
	/**
	 * Compile remote executor results
	 * @param taskStyle could be "mapper" or null for other cases 
	 * @param task
	 * @return true if success and false if fails
	 */
	public boolean compileTaskResults(String taskStyle, RemoteExecutor task);

}
