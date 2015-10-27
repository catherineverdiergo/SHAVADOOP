package com.tpt.shavadoop.tests;

//import static org.junit.Assert.fail;

import java.io.File;
import java.io.FilenameFilter;

import com.tpt.shavadoop.master.Configuration;
import com.tpt.shavadoop.master.remote.RemoteExecutor;
import com.tpt.shavadoop.master.split.ISplitter;
import com.tpt.shavadoop.util.FileUtils;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestMaster1 extends TestCase {

//	@Test
	public void testRemoteExecutor() {
		RemoteExecutor re = new RemoteExecutor("echo $((2+3))");
		re.setHostName("localhost");
		re.start();
		while(re.isAlive()) {
			try {
				Thread.sleep(500);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		Assert.assertTrue("5".equals(re.getLastOutMessage()));
		re.close();
//		fail("Not yet implemented");	
	}

//	@Test
	public void testSplitter1() {
		Configuration.initConfiguration("conf/master.properties");
		ISplitter splitter = Configuration.getSplitter();
		File tmpDir = new File("tmp");
		if (tmpDir.exists() && tmpDir.isDirectory()) {
			try {
				FileUtils.delete(tmpDir);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		splitter.doSplit();
		tmpDir = new File("tmp");
		Assert.assertTrue(tmpDir.exists() && tmpDir.isDirectory());
		String[] myFiles = tmpDir.list(new FilenameFilter() {
		    public boolean accept(File directory, String fileName) {
		        return fileName.startsWith("split-");
		    }
		});
		System.out.println(myFiles.length);
		Assert.assertTrue(myFiles.length==3);
		Configuration.closeConfiguration();
	}

//	@Test
	public void testSplitter2() {
		Configuration.initConfiguration("conf/master2.properties");
		ISplitter splitter = Configuration.getSplitter();
		File tmpDir = new File("tmp");
		if (tmpDir.exists() && tmpDir.isDirectory()) {
			try {
				FileUtils.delete(tmpDir);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		splitter.doSplit();
		tmpDir = new File("tmp");
		Assert.assertTrue(tmpDir.exists() && tmpDir.isDirectory());
		String[] myFiles = tmpDir.list(new FilenameFilter() {
		    public boolean accept(File directory, String fileName) {
		        return fileName.startsWith("split-");
		    }
		});
		System.out.println(myFiles.length);
		Assert.assertTrue(myFiles.length==6);
		Configuration.closeConfiguration();
	}

//	@Test
	public void testRemoteMapper() {
		RemoteExecutor re = new RemoteExecutor("./slave.jar --map WordCounterMapper split-unit-test2");
		re.setHostName("localhost");
		re.start();
		while (re.isAlive()) { 
			try {
				Thread.sleep(500);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		Assert.assertTrue(re.getLastOutMessage().startsWith("<terminated-mapper>"));
		re.close();
	}

}
