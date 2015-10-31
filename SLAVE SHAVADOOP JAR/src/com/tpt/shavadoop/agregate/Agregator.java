package com.tpt.shavadoop.agregate;

import java.io.File;
import java.io.FileFilter;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.slave.Slave;
import com.tpt.shavadoop.util.CommonTags;
import com.tpt.shavadoop.util.FileUtils;

public class Agregator {
	
	private static final Logger logger = Logger.getLogger(Agregator.class);
	
	private String inputFilesPrefix = "RM";
	
	private String inputDirectory = "tmp";
	
	private String finalResultFile = "shavadoop.output";
	
	public String getInputFilesPrefix() {
		return inputFilesPrefix;
	}

	public void setInputFilesPrefix(String inputFilesPrefix) {
		this.inputFilesPrefix = inputFilesPrefix;
	}

	public String getInputDirectory() {
		return inputDirectory;
	}

	public void setInputDirectory(String inputDirectory) {
		this.inputDirectory = inputDirectory;
	}

	public String getFinalResultFile() {
		return finalResultFile;
	}

	public void setFinalResultFile(String finalResultFile) {
		this.finalResultFile = finalResultFile;
	}

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
			System.err.println(CommonTags.TAG_HEART_BEAT+"HeartBeat mapper for file "+inputFile+" ,last token: "+tokenIdx);
			try {
				Thread.sleep(Slave.DEFAULT_HB_DELAY);
			}
			catch (Exception e) {
				logger.error(e,e);
			}
		}
		
	}

	public void doAgregation() {
		File dir = new File(inputDirectory);
		FileFilter ff = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.getName().startsWith(inputFilesPrefix))
					return true;
				else
					return false;
			}
		};
		
		for (File f:dir.listFiles(ff)) {
			try {
				String key = f.getName().substring(f.getName().indexOf("-")+1, f.getName().length());
				FileUtils.copyFile(key, f.getAbsolutePath(), inputDirectory+"/"+finalResultFile, true);
			}
			catch (Exception e) {
				System.err.println(CommonTags.TAG_ERROR_TASK+"Error generating final target file");
			}
		}
	}

}
