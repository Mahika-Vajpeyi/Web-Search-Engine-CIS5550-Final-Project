package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {

	String id;
	long creationTime;
	long lastAccessedTime;
	int maxActiveInterval;
	Map<String, Object> attributes;
	boolean valid;
	
	SessionImpl(String id, long creationTime, long lastAccessedTime, int maxActiveInterval) {
		this.id = id;
		this.creationTime = creationTime;
		this.lastAccessedTime = lastAccessedTime;
		this.maxActiveInterval = maxActiveInterval;
		valid = true;
		attributes = new HashMap<String, Object>();
	}
	
	@Override
	public String id() {
		// TODO Auto-generated method stub
		return id;
	}

	@Override
	public long creationTime() {
		// TODO Auto-generated method stub
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		// TODO Auto-generated method stub
		return lastAccessedTime;
	}
	
	public void setLastAccessedTime(long milliseconds) {
		// TODO Auto-generated method stub
		lastAccessedTime = milliseconds;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		// TODO Auto-generated method stub
		maxActiveInterval = seconds;
	}
	
	public int getMaxActiveInterval() {
		return maxActiveInterval;
	}

	@Override
	public void invalidate() {
		// TODO Auto-generated method stub
		valid = false;
	}

	@Override
	public Object attribute(String name) {
		// TODO Auto-generated method stub
		return attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		// TODO Auto-generated method stub
		attributes.put(name,  value);
	}

}
