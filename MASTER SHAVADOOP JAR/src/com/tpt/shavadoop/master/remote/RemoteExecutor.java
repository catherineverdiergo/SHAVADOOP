package com.tpt.shavadoop.master.remote;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.tpt.shavadoop.util.StreamMessage;

public class RemoteExecutor extends Thread {
	
	private String command;
    private StreamDisplayer fluxSortie;
    private StreamDisplayer fluxErreur;
    private Thread w1;
    private Thread w2;
    private String hostName;
    private String errorMessage;

	public RemoteExecutor(String command) {
		super();
		this.command = command;
	}
	
	public static boolean isProcessTerminated(Process p) {
		boolean result = true;
		try {
			p.exitValue();
		}
		catch (IllegalThreadStateException e) {
			result = false;
		}
		return result;
	}

	@Override
	public void run() {
       try {
    	    if (hostName != null) {
	            ProcessBuilder pb = new ProcessBuilder("sh", "-c", "ssh "+this.hostName+" "+command);
	            Process p = pb.start();
	            this.fluxSortie = new StreamDisplayer(p.getInputStream());
	            this.fluxErreur = new StreamDisplayer(p.getErrorStream());
	            w1 = new Thread(fluxSortie);
	            w1.start();
	            w2 = new Thread(fluxErreur);
	            w2.start();
	            boolean pRunningProperly = true;
	            while (pRunningProperly && !isProcessTerminated(p)) {
	            	Thread.sleep(100);
	            	Date hbDate = fluxErreur.getLastHeartBeatDate();
	            	Date now = new Date();
	            	if (hbDate != null && now.getTime() - hbDate.getTime() > 90000) {
	            		errorMessage = "Error : no more heart beat for "+command;
	            		pRunningProperly = false;
	            	}
	            }
	            if ( pRunningProperly && p.exitValue() != 0) {
	            	errorMessage = "Error : process terminated abnormally ("+command+")";
	            }
	            else if (!pRunningProperly) {
	            	p.destroy();
	            }
    	    }
    	    else {
    	    	errorMessage = "Error : hostName undefined for command "+command;
    	    	return;
    	    }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}
	
	public String getLastOutMessage() {
		while (w1.isAlive());
		if (errorMessage != null) {
			return errorMessage;
		}
		else if (fluxSortie != null) {
			return this.fluxSortie.getLastLine();
		}
		else {
			return null;
		}
	}
	
	public String getLastErrMessage() {
		while (w2.isAlive());
		return this.fluxErreur.getLastLine();
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	
	public void close() {
        w1 = null;
        w2 = null;
	}
	
	public List<StreamMessage> getLastOutMessages() {
		return fluxSortie.getLastLines();
	}
	
	public List<StreamMessage> getLastErrMessages() {
		return fluxErreur.getLastLines();
	}

	public String getCommand() {
		return command;
	}
	
}
