package com.tpt.shavadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

import com.tpt.shavadoop.master.Configuration;

public class CheckPing {
	
	/**
	 * Check if a given host is online and available for ssh connections
	 * @param host
	 * @return boolean : true if ssh if available on host and otherwise false
	 */
	public static boolean checkHost(String host) {
		boolean result = false;
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(new String[] {"ssh", host, "echo $((2+3))"});
			pr.waitFor();
			result = pr.exitValue()==0;
		}catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * Read a list of hosts from an input file and rewrite in an output
	 * those on which ssh connections are possible
	 * @param hostsFile : input file (list of candidate hosts)
	 * @param onlineHostsFile : output file (list of avaliable hosts)
	 */
	public static void checkHosts(String hostsFile, String onlineHostsFile) {
		BufferedReader r = FileUtils.openFile4Read(hostsFile);
		BufferedWriter w = FileUtils.openFile4Write(onlineHostsFile);
		String host = FileUtils.getNextLine(r);
		while (host != null) {
			if (checkHost(host)) {
				FileUtils.write(w,host);
			}
			host = FileUtils.getNextLine(r);			
		}
		FileUtils.close(r);
		FileUtils.close(w);
	}

	/**
	 * Read a list of hosts from an input file and put in an ArrayList
	 * those on which ssh connections are possible
	 * @param hostsFile : input file (list of candidate hosts)
	 * @param onlineHostsFile : output file (list of avaliable hosts)
	 */
	public static List<String> getOnlineHosts(String hostsFile) {
		List<String> result = new ArrayList<String>();
		BufferedReader r = FileUtils.openFile4Read(hostsFile);
		String host = FileUtils.getNextLine(r);
		while (host != null) {
			if (checkHost(host)) {
				result.add(host);
			}
			host = FileUtils.getNextLine(r);			
		}
		FileUtils.close(r);
		return result;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			checkHosts(Configuration.getParameter("hosts.file"),Configuration.getParameter("conf/onlinehosts-133"));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
