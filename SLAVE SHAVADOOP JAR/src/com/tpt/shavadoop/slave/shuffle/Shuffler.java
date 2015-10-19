package com.tpt.shavadoop.slave.shuffle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.ResourceManager;
import com.tpt.shavadoop.slave.Slave;
import com.tpt.shavadoop.util.FileUtils;

public class Shuffler {
	
	private static final Logger logger = Logger.getLogger(ResourceManager.class);

	private List<String> inputFiles = new ArrayList<String>();
	
	private String wordKey;

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
			System.err.println("HeartBeat shuffler for file "+inputFile+" ,key: "+key);
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
			HeartBeat hb = new HeartBeat();
			BufferedWriter bw = FileUtils.openFile4Write("SM-"+wordKey);
			for (String inputFile:inputFiles) {
				BufferedReader br = FileUtils.openFile4Read(inputFile);
				String line = br.readLine();
				while (line != null) {
					if (!hb.isAlive()) {
						hb.setInputFile(inputFile);
						hb.setKey(wordKey);
						hb.start();
					}
					String[] tokens = line.split("[ \t]");
					if (wordKey.equals(tokens[0])) {
						bw.write(line+"\n");
					}
				}
				FileUtils.close(br);
			}
			FileUtils.close(bw);
		}
		catch (Exception e) {
			logger.error(e,e);
		}
	}

}
