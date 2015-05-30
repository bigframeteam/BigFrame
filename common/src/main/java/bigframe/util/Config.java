package bigframe.util;

import java.util.HashMap;
import java.util.Map;

/**
 * A abstract class for for storing configuration.
 * 
 * @author andy
 *
 */
public abstract class Config {
	//private static final Logger LOG = Logger.getLogger(Configuration.class);
	
	
	/**
	 * A map contains a set of properties and their corresponding values.
	 */
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
	
	public String get(String key) {
		if (properties.containsKey(key))
			return properties.get(key);
		else
			return "";
	}
	
	
	public String get(String key, String defaultValue) {
	   if (properties.containsKey(key))
	      return properties.get(key);
	    else
	      return defaultValue;
	}
	
	public Map<String,String> get() {
		return properties;
	}
	
	public void set(Map<String, String> prop) {
		properties = prop;
		reloadConf();
	}
	
	/**
	 * Reload the configuration.
	 */
	protected abstract void reloadConf();
	
	/**
	 * Print out the configuration.
	 */
	public abstract void printConf();
}
