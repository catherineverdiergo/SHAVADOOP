package com.tpt.shavadoop.slave.shuffle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.slave.Slave;
import com.tpt.shavadoop.util.CommonTags;
import com.tpt.shavadoop.util.FileUtils;

public class Shuffle {
	
	private static final Logger logger = Logger.getLogger(Shuffle.class);

	// List of files to shuffle will be written in a file
	private String fileListName;
	
	private String workingDir;

	// Will be filled from data in fileListName 
	private List<String> inputFiles = new ArrayList<String>();
	
	// wordKey to look for during shuffle 
	private String wordKey;
	
	/**
	 * Constructor : construct list of files to shuffle
	 * @param fileListName
	 */
	public Shuffle(String fileListName) {
		super();
		this.fileListName = fileListName.replace("\\\\", "");
		System.out.println(this.fileListName);
		File inFile = new File(fileListName);
		workingDir = inFile.getParent();
		String [] keySplitter = inFile.getName().split("\\.");
		this.wordKey = keySplitter[0];
		BufferedReader br = FileUtils.openFile4Read(this.fileListName);
		try {
			String line = br.readLine();
			while (line !=null) {
				inputFiles.add(line);
				line = br.readLine();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
			logger.error(e,e);
		}
		FileUtils.close(br);
	}

	/**
	 * HeartBeat class : display a message on stderr
	 * @author developer
	 *
	 */
	class HeartBeat extends Thread {
		
		private String key;
		private String inputFile;
		
		public void setKey(String key) {
			this.key = key;
		}

		public void setInputFile(String inputFile) {
			this.inputFile = inputFile;
		}

		@Override
		public void run() {
			System.err.println(CommonTags.TAG_HEART_BEAT+"HeartBeat shuffler for file "+inputFile+" ,key: "+key);
			try {
				Thread.sleep(Slave.DEFAULT_HB_DELAY);
			}
			catch (Exception e) {
				logger.error(e,e);
			}
		}
		
	}

	public void setWordKey(String wordKey) {
		this.wordKey = wordKey;
	}
	
	public void addInputFile(String inputFile) {
		this.inputFiles.add(inputFile);
	}
	
	public void doShuffle() {
		try {
			HeartBeat hb = null;
			String destFile = this.workingDir+"/SM-"+wordKey;
			destFile.replace(" ", "\\ ");
			BufferedWriter bw = FileUtils.openFile4Write(destFile);
			for (String inputFile:inputFiles) {
				BufferedReader br = FileUtils.openFile4Read(inputFile.replace("\\\\", ""));
				String line = br.readLine();
				while (line != null) {
					if (hb == null || !hb.isAlive()) {
						hb = new HeartBeat();
						hb.setInputFile(new File(inputFile.replace("\\\\", "")).getName());
						hb.setKey(wordKey);
						hb.start();
					}
					String[] tokens = line.split("[ \t]");
					if (wordKey.equals(tokens[0])) {
						bw.write(line+"\n");
					}
					line = br.readLine();
					// Delay task to be able to test heart beat
					if (Slave.DEFAULT_TASK_DELAY != -1) {
						try {
							Thread.sleep(Slave.DEFAULT_TASK_DELAY);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				FileUtils.close(br);
			}
			FileUtils.close(bw);
			System.err.println(CommonTags.TAG_FINISHED_TASK+"Shuffle terminated");
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.error(e,e);
			System.err.println(CommonTags.TAG_ERROR_TASK+"IO error during shuffle");
			System.exit(-1);
		}
	}

	public void setFileListName(String fileListName) {
		this.fileListName = fileListName;
		File file = new File(fileListName);
		// retrieve key from file name
		String[] data = file.getName().split(".");
		this.setWordKey(data[0]);
		BufferedReader br = FileUtils.openFile4Read(fileListName);
		try {
			String fName = br.readLine();
			while (fName != null) {
				this.addInputFile(fName);
				fName = br.readLine();
			}
		}
		catch (Exception e) {
			System.err.println(CommonTags.TAG_ERROR_TASK+"IO error for shuffle");
			System.exit(-1);
		}
	}

}
