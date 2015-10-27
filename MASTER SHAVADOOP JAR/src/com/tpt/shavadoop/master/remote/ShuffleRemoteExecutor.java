package com.tpt.shavadoop.master.remote;

public class ShuffleRemoteExecutor extends RemoteExecutor {
	
	private String wordKey;

	public ShuffleRemoteExecutor(String command) {
		super(command);
	}

	public String getWordKey() {
		return wordKey;
	}

	public void setWordKey(String wordKey) {
		this.wordKey = wordKey;
	}

}
