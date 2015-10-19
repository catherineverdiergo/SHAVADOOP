package com.tpt.shavadoop.test;

import java.io.File;

import junit.framework.Assert;

import com.tpt.shavadoop.slave.Slave;
import com.tpt.shavadoop.slave.map.WordCounterMapper;

public class TestSlave1 {
	
//	@Test
	public static void testWordCount(String inputFile, String outputFile) {
		File oFile = new File(outputFile);
		if (oFile.exists()) {
			oFile.delete();
		}
		String[] args = new String[3];
		args[0] = Slave.MAP_TASK;
		args[1] = WordCounterMapper.class.getSimpleName();
		args[2] = inputFile;
		Slave.main(args);
		oFile = new File(outputFile);
		Assert.assertTrue(oFile.exists());
	}

//	@Test
	public void testMapper1() {
		testWordCount("tmp/split-unit-test1", "tmp/UM-unit-test1");
	}

//	@Test
	public void testMapper2() {
		testWordCount("tmp/split-unit-test2", "tmp/UM-unit-test2");
	}

}
