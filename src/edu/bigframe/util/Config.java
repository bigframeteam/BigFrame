package edu.bigframe.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 
 * @author andy
 *
 */
public abstract class Config {
	//private static final Logger LOG = Logger.getLogger(Configuration.class);
	
	protected Map<String,String> properties;
	
	public Config() {
		properties = new HashMap<String, String>();
	}
	
	public void set(String key, String value) {
		properties.put(key, value);
		reloadConf();
	}
	
	public Map<String,String> getProp() {
		return properties;
	}
	
	public void set(Map<String, String> prop) {
		properties = prop;
		reloadConf();
	}
	
	protected abstract void reloadConf();
	public abstract void printConf();
}
