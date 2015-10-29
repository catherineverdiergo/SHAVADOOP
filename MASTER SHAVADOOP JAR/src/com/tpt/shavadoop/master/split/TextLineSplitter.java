package com.tpt.shavadoop.master.split;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.Configuration;
import com.tpt.shavadoop.util.FileUtils;

public class TextLineSplitter implements ISplitter {
	
	private String fileName;
	private int nbLines;
	
	private static final Logger logger = Logger.getLogger(TextLineSplitter.class);
	
	public TextLineSplitter(String fileName) {
		super();
		this.fileName = fileName;
		try {
			this.nbLines = Integer.parseInt(Configuration.getParameter("splitter.block_size"));
		}
		catch (Exception e) {
			this.nbLines = 1;
		}
	}

	@Override
	public void doSplit() {
		BufferedReader br = FileUtils.openFile4Read(this.fileName);
		try {
			String line = br.readLine();
			while (line != null) {
				try {
					String destDir = Configuration.getWorkingDir();
					File f = new File(destDir);
					if (!f.exists()) {
						f.mkdirs();
					}
					String splitFileName = String.format(destDir+"/split-"+UUID.randomUUID().toString());
					BufferedWriter bw = FileUtils.openFile4Write(splitFileName);
					int i = 0;
					while (i<this.nbLines && line != null) {
						bw.write(line+System.lineSeparator());
						line = br.readLine();
						i++;
					}
					FileUtils.close(bw);
				}
				catch (Exception e) {
					logger.error(e,e);
				}
			}
		}
		catch (IOException e) {
			logger.error(e,e);
		}
		FileUtils.close(br);
	}
	
}
