package com.tpt.shavadoop.master.split;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.master.Configuration;
import com.tpt.shavadoop.util.FileUtils;

public class TextBlockSplitter implements ISplitter {
	
	private String fileName;
	
	private String baseName;
	
	private long size;
	
	private int blockSize=64000;
	
	private long estimatedNumberOfBlocks;
	
	private String blockDirectoryName="temp";
	
	private int numberOfThreads=4;
	
	private BufferedInputStream[] fHandlers;
	
	private byte[][] readers;
	
	private Splitter[] splitters;
	
	private static final Logger logger = Logger.getLogger(TextBlockSplitter.class);
	
	class Splitter implements Runnable {
		
		long blockNumber;
		
		int index;
		
		public void setBlockNumber(long blockNumber) {
			this.blockNumber = blockNumber;
		}

		public void setIndex(int index) {
			this.index = index;
		}


		public void run() {
			genBlockFile(this.blockNumber, index);
		}
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public String getBlockDirectoryName() {
		return blockDirectoryName;
	}

	public void setBlockDirectoryName(String blockDirectoryName) {
		this.blockDirectoryName = blockDirectoryName;
	}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public void setNumberOfThreads(int numberOfThreads) {
		this.numberOfThreads = numberOfThreads;
	}

	public TextBlockSplitter(String fileName) {
		super();
		this.fileName = fileName;
		File f = new File(fileName);
		if (f.exists() && f.isFile()) {
			this.size = f.length();
			this.estimatedNumberOfBlocks = this.size/this.blockSize;
			if (this.size%this.blockSize >0) {
				this.estimatedNumberOfBlocks++;
			}
			this.baseName = f.getName();
		}
	}
	
	public long getEstimatedNumberOfBlocks() {
		return estimatedNumberOfBlocks;
	}

	public void init() {
		try {
			FileInputStream fis = new FileInputStream(new File(this.fileName));
			fHandlers = new BufferedInputStream[this.numberOfThreads];
			readers = new byte[this.numberOfThreads][this.blockSize];
			splitters = new Splitter[this.numberOfThreads];
			for (int i=0;i<this.numberOfThreads;i++) {
				fHandlers[i] = new BufferedInputStream(fis);
				splitters[i] = new Splitter();
				splitters[i].setIndex(i);
			}
		}
		catch (FileNotFoundException e) {
			try {
				logger.error(Configuration.getMessage("file.notfound: "+fileName),e);
			}
			catch (Exception ee) {
				logger.error(e,e);
			}
		}
	}
	
	public void close() {
		try {
			for (int i=0;i<this.numberOfThreads;i++) {
				fHandlers[i].close();
			}
		}
		catch (IOException e) {
			try {
				logger.error(Configuration.getMessage("file.errclose: "+fileName),e);
			}
			catch (Exception ee) {
				logger.error(e,e);
			}
		}
	}
	
	private int leftShiftBuffer(byte[] buffer, int nbBytes) {
		int result = -1;
		if (nbBytes > 0) {
			int i = 0;
			byte b = buffer[0];
			while (b != '\n' && i < nbBytes) {
				i++;
				if (i < nbBytes) {
					b = buffer[i];
				}
			}
			for (int j=i+1;j<nbBytes;j++) {
				buffer[j-i-1] = buffer[j];
			}
			result = nbBytes-i-1;
		}
		return result;
	}
	
	private void genBlockFile(long blockNumber, int fHandlerIdx) {
		try {
			int offset = (int)(blockNumber*this.blockSize);
			int nbRead = this.fHandlers[fHandlerIdx].read(readers[fHandlerIdx], offset, this.blockSize);
			if (nbRead != -1) {
				nbRead = leftShiftBuffer(readers[fHandlerIdx], nbRead);
				String splitFileName = String.format(this.blockDirectoryName+"/"+this.baseName+"-split-%5d", blockNumber);
				BufferedOutputStream bos = FileUtils.openBinFile4Write(splitFileName);
				bos.write(readers[fHandlerIdx], 0, nbRead);
				offset += this.blockSize;
				nbRead = this.fHandlers[fHandlerIdx].read(readers[fHandlerIdx], offset, this.blockSize);
				if (nbRead != -1) {
					int i=0;
					byte b = readers[fHandlerIdx][i];
					while (b != '\n' && i<nbRead) {
						bos.write(b);
						i++;
						if (i<nbRead) {
							b = readers[fHandlerIdx][i];
						}
					}
					if (i<nbRead) {
						bos.write(b);
					}
				}
				FileUtils.close(bos);
			}
		}
		catch (IOException e) {
			try {
				logger.error(Configuration.getMessage("file.errread: "+fileName),e);
			}
			catch (Exception ee) {
				logger.error(e,e);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.tpt.shavadoop.master.split.ISplitter#doSplit()
	 */
	@Override
	public void doSplit() {
		int lastBlockGenerated = -1;
		while (lastBlockGenerated < this.estimatedNumberOfBlocks) {
			
		}
	}
	
}
