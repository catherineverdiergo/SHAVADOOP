package com.tpt.shavadoop.master.task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.Configuration;
import com.tpt.shavadoop.master.TaskManager;
import com.tpt.shavadoop.master.remote.AgregatorRemoteExecutor;
import com.tpt.shavadoop.master.remote.ReducerRemoteExecutor;
import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.master.remote.ShuffleRemoteExecutor;
import com.tpt.shavadoop.util.CommonTags;
import com.tpt.shavadoop.util.FileUtils;
import com.tpt.shavadoop.util.StreamMessage;

public class WordCountTaskSelectector implements ITaskSelector {
	
	private HashMap<String,List<String>> keyDictionnary = new HashMap<String,List<String>>();
	
	private HashMap<String,String> simpleKeyDictionnary = new HashMap<String,String>();

	private static final Logger logger = Logger.getLogger(WordCountTaskSelectector.class);
	
	private boolean hasNextCandidate = true;
	
	/**
	 * This method is called by TaskManager once all mappers have been completed
	 */
	@Override
	public boolean hasNextCandidate() {
		return hasNextCandidate;
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
		String keysLine = messages.get(messages.size()-2).getMsg();
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

	/**
	 * Compile task results :
	 * First : check the stderr to be sure task has been properly completed
	 * Then : parse the stdout to get task results if any 
	 */
	@Override
	public boolean compileTaskResults(String taskStyle, RemoteExecutor task) {
		boolean result = true;
		// Check if task completed
		result = parseTaskErrResult(task);
		if (result) {
			if ("mapper".equals(taskStyle)) {
				parseMapperOutResult(task);
			}
			else if ("shuffle".equals(taskStyle)) {
				// Create related key reducer task
				// retrieve word key
				String wordKey = ((ShuffleRemoteExecutor)task).getWordKey();
				String reducerCommand = Configuration.getParameter("slave.prg")+" --reduce ";
				reducerCommand += Configuration.getParameter("slave.reducerClass");
				reducerCommand += " "+FileUtils.addBackspaces(new File(Configuration.getWorkingDir()).getAbsolutePath())+" "+wordKey;
				ReducerRemoteExecutor rExecutor = new ReducerRemoteExecutor(reducerCommand);
				TaskManager.addTask(rExecutor);
			}
			// Do nothing when reducer and no error
			else if ("agregator".equals(taskStyle)) {
				// If agregator finished properly, we can stop TaskManager
				hasNextCandidate = false;
			}
		}
		return result;
	}
	
	/**
	 * Generate data files from key dictionary
	 * and related shuffle tasks
	 */
	private void genShuffleTasks() {
		String tmpDir = Configuration.getWorkingDir();
		Iterator<String> itKeys = keyDictionnary.keySet().iterator();
		while (itKeys.hasNext()) {
			// Create a file for a dedicated key-shuffle task
			// That file will hold all shuffle input files for
			// a specific key word 
			String key = itKeys.next();
			String fileName = new File(tmpDir).getAbsolutePath()+"/"+key+".files";
			BufferedWriter bw = FileUtils.openFile4Write(fileName);
			List<String> keyUMFiles = keyDictionnary.get(key);
			for (String UMFile:keyUMFiles) {
				try {
					bw.write(UMFile+"\n");
				}
				catch (IOException e) {
					logger.error(e,e);
				}
			}
			FileUtils.close(bw);
			simpleKeyDictionnary.put(key, fileName);
			// Create related shuffle task
			String shuffleCommand = Configuration.getParameter("slave.prg")+" --shuffle ";
			shuffleCommand += " \""+FileUtils.addBackspaces(fileName)+"\"";
			ShuffleRemoteExecutor rShuffle = new ShuffleRemoteExecutor(shuffleCommand);
			rShuffle.setWordKey(key);
			TaskManager.addTask(rShuffle);
		}
	}


	/* (non-Javadoc)
	 * @see com.tpt.shavadoop.master.task.ITaskVoter#prepare4Shuffle()
	 */
	@Override
	public void prepare4Shuffle() {
		// Create an entry file for each Shuffle
		this.genShuffleTasks();
	}

	/* (non-Javadoc)
	 * @see com.tpt.shavadoop.master.task.ITaskSelector#beforeJobEnd()
	 */
	@Override
	public void beforeJobEnd() {
		if (hasNextCandidate) {
			// Create agregator task and add it to TaskManager's task queue
			String aCommand = Configuration.getParameter("slave.prg")+" --agregate "
					+ Configuration.getParameter("slave.agregator")+" "
					+FileUtils.addBackspaces(new File(Configuration.getWorkingDir()).getAbsolutePath());
			AgregatorRemoteExecutor agregator = new AgregatorRemoteExecutor(aCommand);
			TaskManager.addTask(agregator);
		}
	}


}
