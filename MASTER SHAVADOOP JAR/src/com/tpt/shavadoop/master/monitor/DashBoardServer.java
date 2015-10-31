/**
 * 
 */
package com.tpt.shavadoop.master.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.log4j.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.tpt.shavadoop.master.Configuration;
import com.tpt.shavadoop.master.TaskManager;
/**
 * @author catherine
 *
 */
public class DashBoardServer extends Thread {

	private static final Logger logger = Logger.getLogger(DashBoardServer.class);
	
	/*
	 * a simple static http server
	*/
	public static void main(String[] args) throws Exception {
		new DashBoardServer().start();
	}
	  
	static class MyHandler implements HttpHandler {
		public void handle(HttpExchange t) throws IOException {
			String response = "Welcome Real's HowTo test page";
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}
	
	static class MyRestHandler implements HttpHandler {
		public void handle(HttpExchange t) throws IOException {
			URI uri = t.getRequestURI();
			String response = "";
			if (uri.toString().endsWith("status")) {
				response = TaskManager.getStatus();
			}
			else if (uri.toString().endsWith("result")) {
				response = TaskManager.showResult();
			}
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}
	
	static class MyFileHandler implements HttpHandler {

		@Override
		public void handle(HttpExchange t) throws IOException {
			String root = Configuration.getParameter("monitor.root");
			root = new File(root).getCanonicalPath();
			URI uri = t.getRequestURI();
			String realPath = uri.getPath().replace(Configuration.getParameter("monitor.maproot"), root);
			File file = new File(realPath).getCanonicalFile();
			if (!file.getPath().startsWith(root)) {
				// Suspected path traversal attack: reject with 403 error.
				String response = "403 (Forbidden)\n";
				t.sendResponseHeaders(403, response.length());
				OutputStream os = t.getResponseBody();
				os.write(response.getBytes());
				os.close();
			}
			else if (!file.isFile()) {
				// Object does not exist or is not a file: reject with 404 error.
				String response = "404 (Not Found)\n";
				t.sendResponseHeaders(404, response.length());
				OutputStream os = t.getResponseBody();
				os.write(response.getBytes());
				os.close();
			} 
			else {
				// Object exists and is a file: accept with response code 200.
				t.sendResponseHeaders(200, 0);
				OutputStream os = t.getResponseBody();
				FileInputStream fs = new FileInputStream(file);
				final byte[] buffer = new byte[0x10000];
				int count = 0;
				while ((count = fs.read(buffer)) >= 0) {
					os.write(buffer,0,count);
				}
				fs.close();
				os.close();
			}
		}
	}
	
	@Override
	public void run() {
		HttpServer server;
		try {
			server = HttpServer.create(new InetSocketAddress(Integer.parseInt(Configuration.getParameter("monitor.port"))), 0);
			server.createContext("/test", new MyHandler());
			server.createContext("/shavadoop/rest/status", new MyRestHandler());
			server.createContext(Configuration.getParameter("monitor.maproot"), new MyFileHandler());
			server.setExecutor(null); // creates a default executor
			server.start();
		} catch (IOException e) {
			logger.error(e,e);
		}
	}
	
	
}