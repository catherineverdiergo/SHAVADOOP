package com.tpt.shavadoop.master.remote;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.tpt.shavadoop.util.StreamMessage;

public class StreamDisplayer implements Runnable {

    protected final InputStream inputStream;
    
    protected List<StreamMessage> lastLines = new ArrayList<StreamMessage>();
    
	private static final Logger logger = Logger.getLogger(StreamDisplayer.class);

	StreamDisplayer(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    protected BufferedReader getBufferedReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public void run() {
        BufferedReader br = getBufferedReader(inputStream);
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                logger.info("receive message from task: "+line);
                if (lastLines.size() >= 10) {
                	this.lastLines.remove(0);
                }
            	this.lastLines.add(new StreamMessage(line, new Date()));
            }
        } catch (IOException e) {
            logger.error(e,e);
        }
    }

	public String getLastLine() {
		return lastLines.get(lastLines.size()-1).getMsg();
	}
	
	public Date getLastHeartBeatDate() {
		Date result = null;
		if (lastLines.size() > 0) {
			StreamMessage lastsm = lastLines.get(lastLines.size()-1);
			if (lastsm!= null && lastsm.getMsg().startsWith("HeartBeat")) {
				result = lastsm.getDt();
			}
		}
		return result;
	}

	public List<StreamMessage> getLastLines() {
		return lastLines;
	}

}
