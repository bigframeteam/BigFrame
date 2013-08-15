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
	
	public Map<String,String> getProp() {
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
