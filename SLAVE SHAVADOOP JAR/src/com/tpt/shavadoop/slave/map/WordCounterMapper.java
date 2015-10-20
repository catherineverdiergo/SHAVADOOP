package com.tpt.shavadoop.slave.map;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.util.CommonTags;

public class WordCounterMapper extends AbstractMapper {

	private static final Logger logger = Logger.getLogger(WordCounterMapper.class);
	
	private Set<String> wordsFound = new HashSet<String>();

	/* (non-Javadoc)
	 * @see com.tpt.shavadoop.slave.map.AbstractMapper#getNextToken(java.io.BufferedReader)
	 */
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

	/* Implements an abstract method
	 * @see com.tpt.shavadoop.slave.map.AbstractMapper#doCustomMap(java.lang.Object, java.io.BufferedWriter)
	 * Add a map entry in output file for each word found
	 */
	@Override
	void doCustomMap(Object inputToken, BufferedWriter outWriter) {
		try {
			String line = (String)inputToken;
			line.toUpperCase();
			String[] words = line.split("([\\W\\s]+)");
			for (String w:words) {
				if (!"".equals(w)) {
					outWriter.write(w+" 1\n");
					wordsFound.add(w);
				}
			}
		}
		catch (IOException e) {
			logger.error(e,e);
		}
	}

	/* Implements an abstract method
	 * @see com.tpt.shavadoop.slave.map.AbstractMapper#displayMessage2Master()
	 * return the list of words found to master on stdout
	 */
	@Override
	void displayMessage2Master() {
		Iterator<String> it = wordsFound.iterator();
		while (it.hasNext()) {
			System.out.print(it.next());
			if (it.hasNext()) {
				System.out.print(",");
			}
		}
		System.out.println();
		System.err.println(CommonTags.TAG_FINISHED_TASK+this.getClass().getSimpleName()+" at "+new Date().toString());
	}

	@Override
	void addGlobalInformation(BufferedWriter outWriter) {
		// do nothing for word counter
	}
	
}
