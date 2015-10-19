package com.tpt.shavadoop.tests;

//import static org.junit.Assert.fail;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.tpt.shavadoop.master.remote.RemoteExecutor;

public class TestMaster2 extends TestCase {

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
	public void testKillProcessErrorManagement() {
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
		Assert.assertTrue(re.getLastOutMessage().startsWith("Error : process terminated abnormally"));
		re.close();
	}

//	@Test
	public void testHeartBeatErrorManagement() {
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
		assertTrue(re.getLastOutMessage().startsWith("Error : no more heart beat for "));
		re.close();
	}
	
}
