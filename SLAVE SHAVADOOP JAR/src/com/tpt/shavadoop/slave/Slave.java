package com.tpt.shavadoop.slave;

import java.io.File;

import com.tpt.shavadoop.slave.map.AbstractMapper;

public class Slave {
	
	/**
	 * Available values to define kind of task to be played by the slave
	 */
	public static final String MAP_TASK = "--map";
	public static final String SHUFFLE_TASK = "--shuffle";
	public static final String REDUCE_TASK = "--reduce";
	
	/*
	 * Default delay for heartbeats
	 */
	public static final int DEFAULT_HB_DELAY = 1000;
	
	/*
	 * Delay task in order to be able to test heart beat
	 * not used if set to -1
	 */
	public static final int DEFAULT_TASK_DELAY = 2;
	

	/*
	// task to be performed (could be MAP_TASK|SHUFFLE_TASK|REDUCE_TASK)
	private String currentTask;
	// some tasks such as map or reduce could be customized, depending
	// on which kind of operation to perform on data
	private String taskClassName;
	
	public void setCurrentTask(String currentTask) {
		this.currentTask = currentTask;
	}

	public void setTaskClassName(String taskClassName) {
		this.taskClassName = taskClassName;
	}
*/
	public static void main(String[] args) {
		// check parameters
		if (args.length < 2) {
			System.err.println("Usage error : expected Slave + option (--map|--shuffle|--reduce) + {mapperClassName} + inputFileName");
			System.exit(-1);
		}
		else {
			// Check option
			if (MAP_TASK.equals(args[0])) {
				// Map task should have a custom mapper class given as args[1]
				if (args.length < 3) {
					System.err.println("Usage error : expected Slave + --map + mapperClassName + inputFileName");
					System.exit(-1);
				}
				else {
					// check if class exists in package com.tpt.shavadoop.slave.map
					String mapperClassName = "com.tpt.shavadoop.slave.map."+args[1];
					try {
						AbstractMapper mapper = (AbstractMapper)Class.forName(mapperClassName).newInstance();
						File iFile = new File(args[2]);
						String iDirName = ".";
						if (iFile.getParent()!=null) {
							iDirName = iFile.getParent().toString();
						}
						String iBaseName = iFile.getName();
						String uuid = iBaseName.substring("split-".length(),iBaseName.length());
						String outputFile = iDirName+"/UM-"+uuid;
						mapper.doMap(args[2], outputFile);
					} catch (InstantiationException | IllegalAccessException
							| ClassNotFoundException e) {
						System.err.println("Error : unknown mapper class : "+mapperClassName);
						System.exit(-1);
					}
				}
			}
			else if (SHUFFLE_TASK.equals(args[0])) {
				
			}
		}
	}

}
