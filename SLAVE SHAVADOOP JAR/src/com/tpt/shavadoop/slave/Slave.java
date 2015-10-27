package com.tpt.shavadoop.slave;

import java.io.File;

import com.tpt.shavadoop.slave.map.AbstractMapper;
import com.tpt.shavadoop.slave.reduce.AbstractReducer;
import com.tpt.shavadoop.slave.shuffle.Shuffle;
import com.tpt.shavadoop.util.CommonTags;

public class Slave {
	
	/**
	 * Available values to define kind of task to be played by the slave
	 */
	public static final String HELP = "--help";
	public static final String MAP_TASK = "--map";
	public static final String SHUFFLE_TASK = "--shuffle";
	public static final String REDUCE_TASK = "--reduce";
	public static final String AGREGATE_TASK = "--agregate";
	
	/*
	 * Default delay for heartbeats
	 */
	public static final int DEFAULT_HB_DELAY = 1000;
	
	/*
	 * Delay task in order to be able to test heart beat
	 * not used if set to -1
	 */
	public static final int DEFAULT_TASK_DELAY = 2;
	

	private static void help() {
		System.out.println("Usage for salve.jar:");
		System.out.println("    salve.jar --help                                     ==> result you got currently");
		System.out.println("    salve.jar --map + mapper class + file to map         ==> map a file");
		System.out.println("    salve.jar --shuffle + key.files                      ==> shuffle for a given list of file in key.files file");
		System.out.println("    salve.jar --reduce + reducer class + workdir + key   ==> reduce for a given key");
		System.out.println("    salve.jar --agregate + workdir                       ==> agregate all RMx files in directory dir");
		System.out.println("Result of agregate task is stored in dir/shavadoop.results file");
	}
	
	public static void main(String[] args) {
		// check parameters
		if (args.length < 2) {
			System.err.println("Usage error");
			help();
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
					String mapperClassName = args[1];
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
						System.err.println(CommonTags.TAG_ERROR_TASK+"Error : unknown mapper class : "+mapperClassName);
						System.exit(-1);
					}
				}
			}
			else if (SHUFFLE_TASK.equals(args[0])) {
				new Shuffle(args[1]).doShuffle();
			}
			else if (REDUCE_TASK.equals(args[0])) {
				if (args.length < 4) {
					System.err.println("Usage error : expected Slave + --reduce + reducer class + workdir + key   ==> reduce for a given key");
					System.exit(-1);
				}
				else {
					String reducerClassName = args[1];
					try {
						AbstractReducer reducer = (AbstractReducer)Class.forName(reducerClassName).newInstance();
						File iFile = new File(args[2]);
						String iDirName = ".";
						if (iFile.getParent()!=null) {
							iDirName = iFile.getParent().toString();
						}
						String fName = "SM-"+args[3];
						reducer.doReduce(iDirName+"/"+fName, iDirName+"/RM-"+args[3]);
					} catch (InstantiationException | IllegalAccessException
							| ClassNotFoundException e) {
						System.err.println(CommonTags.TAG_ERROR_TASK+ "Error : unknown reducer class : "+reducerClassName);
						System.exit(-1);
					}
				}
			}
			else if (AGREGATE_TASK.equals(args[0])) {
				
			}
			else if (HELP.equals(args[0])) {
				help();
			}
		}
	}

}
