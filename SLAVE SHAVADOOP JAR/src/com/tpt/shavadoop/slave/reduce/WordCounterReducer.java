package com.tpt.shavadoop.slave.reduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.util.CommonTags;

public class WordCounterReducer extends AbstractReducer {

	private static final Logger logger = Logger.getLogger(WordCounterReducer.class);
	private int counter = 0;
	
	@Override
	Object getNextToken(BufferedReader inReader) {
		Object result = null;
		try {
			result = inReader.readLine();
		}
		catch (Exception e) {
			logger.error(e,e);
		}
		return result;
	}

	@Override
	void doCustomReduce(Object inputToken, BufferedWriter outWriter) {
		this.counter++;
	}

	@Override
	void displayMessage2Master() {
		System.out.println(counter);
		System.err.println(CommonTags.TAG_FINISHED_TASK+this.getClass().getSimpleName()+" at "+new Date().toString());
	}

	@Override
	void addGlobalInformation(BufferedWriter outWriter) {
		try {
			outWriter.write(""+counter+"\n");
		}
		catch (IOException e) {
			System.err.println(CommonTags.TAG_ERROR_TASK+"IO error");
		}
	}

}
