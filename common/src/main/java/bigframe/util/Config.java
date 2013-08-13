package bigframe.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author andy
 *
 */
public abstract class Config {
	//private static final Logger LOG = Logger.getLogger(Configuration.class);
	
	protected Map<String,String> properties = new HashMap<String, String>();
	
	public Config() {
		
	}
	
	public Config(Map<String,String> properties) {
		this.properties = properties;
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
