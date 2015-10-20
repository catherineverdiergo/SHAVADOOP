/**
 * AbstractMapper : abstract class to perform a map task
 * Should be derived to carry out a specific map algorithm
 */
package com.tpt.shavadoop.slave.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.slave.Slave;
import com.tpt.shavadoop.util.CommonTags;
import com.tpt.shavadoop.util.FileUtils;

public abstract class AbstractReducer {

	private static final Logger logger = Logger.getLogger(AbstractReducer.class);
	
	/**
	 * Should be implemented : retrieve next token data from input flow
	 * @param inReader
	 * @return
	 */
	abstract Object getNextToken(BufferedReader inReader);
	
	/**
	 * Should be implemented : process reduce task for an input token
	 * @param inputToken
	 * @param outWriter
	 */
	abstract void doCustomReduce(Object inputToken, BufferedWriter outWriter);
	
	/**
	 * If necessary add a consolidated information in output file
	 * @param outWriter
	 */
	abstract void addGlobalInformation(BufferedWriter outWriter);

	/**
	 * Should be implemented (but can be empty) : display a result message
	 * for the master
	 */
	abstract void displayMessage2Master();
	
	/**
	 * HeartBeat class : display a message on stderr
	 * @author developer
	 *
	 */
	class HeartBeat extends Thread {
		
		private int tokenIdx;
		private String inputFile; 

		public HeartBeat(String inputFile) {
			super();
			this.inputFile = inputFile;
		}

		public void setTokenIdx(int tokenIdx) {
			this.tokenIdx = tokenIdx;
		}

		@Override
		public void run() {
			System.err.println(CommonTags.TAG_HEART_BEAT+"HeartBeat reducer for file "+inputFile+" ,last token: "+tokenIdx);
			try {
				Thread.sleep(Slave.DEFAULT_HB_DELAY);
			}
			catch (Exception e) {
				logger.error(e,e);
			}
		}
		
	}

	/**
	 * Process reduce task
	 * @param inputFile
	 * @param outputFile
	 */
	public void doReduce(String inputFile, String outputFile) {
		HeartBeat hb = null;
		BufferedReader br = FileUtils.openFile4Read(inputFile);
		BufferedWriter bw = FileUtils.openFile4Write(outputFile);
		Object token = getNextToken(br);
		int i = 0;
		while (token != null) {
			i++;
			doCustomReduce(token, bw);
			if (hb == null || !hb.isAlive()) {
				hb = new HeartBeat(inputFile);
				hb.setTokenIdx(i);
				hb.start();
			}
			token = getNextToken(br);
			// Delay task to be able to test heart beat
			if (Slave.DEFAULT_TASK_DELAY != -1 && (i%3) == 0) {
				try {
					Thread.sleep(Slave.DEFAULT_TASK_DELAY);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		FileUtils.close(br);
		addGlobalInformation(bw);
		FileUtils.close(bw);
		displayMessage2Master();
		// Add message to give information about output file
		System.out.println(CommonTags.TAG_OUTPUT_FILE+outputFile);
	}

}
