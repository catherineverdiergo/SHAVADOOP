package com.tpt.shavadoop.master.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.util.CommonTags;
import com.tpt.shavadoop.util.StreamMessage;

public class WordCountTaskVoter implements ITaskVoter {
	
	private HashMap<String,List<String>> keyDictionnary = new HashMap<String,List<String>>();
	
	@Override
	public RemoteExecutor getNextCandidate() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/**
	 * Parse the error output of a RemoteExecutor
	 * @param remoteExecutor
	 * @return : true if the task succeded and false in other cases
	 */
	private boolean parseTaskErrResult(RemoteExecutor remoteExecutor) {
		boolean result = true;
		// Get last outputs from the task
		List<StreamMessage> messages = remoteExecutor.getLastErrMessages();
		if (messages.size() != 0) {
			String lastMessage = messages.get(messages.size()-1).getMsg();
			if (! lastMessage.startsWith(CommonTags.TAG_FINISHED_TASK)) {
				result = false;
			}
		}
		return result;
	}
	
	/**
	 * Parse the result of a mapper task
	 * @param mapperExecutor
	 */
	private void parseMapperOutResult(RemoteExecutor mapperExecutor) {
		// Get last outputs from the task
		List<StreamMessage> messages = mapperExecutor.getLastOutMessages();
		// Parse last line to get output file
		String lastLine = messages.get(messages.size()-1).getMsg();
		String outputFile = lastLine.substring(CommonTags.TAG_OUTPUT_FILE.length());
		// Get line in which keys are written
		String keysLine = messages.get(messages.size()-3).getMsg();
		String[] keys = keysLine.split("[ \t,]");
		for (String key:keys) {
			List<String> files4Key = keyDictionnary.get(key);
			if (files4Key == null) {
				files4Key = new ArrayList<String>();
				keyDictionnary.put(key, files4Key);
			}
			files4Key.add(outputFile);
		}
	}

	@Override
	public boolean compileTaskResults(String taskStyle, RemoteExecutor task) {
		boolean result = true;
		// Check if task completed
		result = parseTaskErrResult(task);
		if (result) {
			if ("mapper".equals(taskStyle)) {
				parseMapperOutResult(task);
			}
		}
		return result;
	}

}
