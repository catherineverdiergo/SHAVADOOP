package com.tpt.shavadoop.util;

import java.util.Date;

public class StreamMessage {

	private String msg;
	private Date dt;
	
	public StreamMessage(String msg, Date dt) {
		super();
		this.msg = msg;
		this.dt = dt;
	}

	public String getMsg() {
		return msg;
	}
	
	public void setMsg(String msg) {
		this.msg = msg;
	}
	
	public Date getDt() {
		return dt;
	}
	
	public void setDt(Date dt) {
		this.dt = dt;
	}

}
