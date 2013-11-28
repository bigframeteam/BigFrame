package bigframe.bigif;

import java.util.HashMap;
import java.util.Map;

import bigframe.util.Config;
import bigframe.util.Constants;

/**
 * A class contains all the information about the query specification 
 * input by users. 
 * 
 * @author andy
 *
 */
public class BigQueryInputFormat extends Config {
	protected String app_domain;
	
	protected String queryVariety;
	protected String queryVelocity;
	protected Integer queryVolume;
	protected Map<String, String> queryRunningEngine = new HashMap<String, String>();
	
	public BigQueryInputFormat() {
		super();

	}
	
	
	public String getQueryVariety() {
		return queryVariety;
	}
	
	public Integer getQueryVolume() {
		return queryVolume;
	}
	
	public String getQueryVelocity() {
		return queryVelocity;
	}
	
	public Map<String, String> getQueryRunningEngine() {
		return queryRunningEngine;
	}

	@Override
	public void printConf() {
		System.out.println("Query Generation configuration:");

		System.out.println("Query variety:" + queryVariety);
		System.out.println("Query velocity:" + queryVelocity);
		System.out.println("Query volume:" + queryVolume);
		System.out.println("Query Run-time engine:");
		for (Map.Entry<String, String> entry : queryRunningEngine.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println("\t" + key +":" + value);
		}

	}

	@Override
	protected void reloadConf() {
		
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue().trim();
			
			if (key.equals(BigConfConstants.BIGFRAME_QUERYVARIETY)) {
				queryVariety = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYVELOCITY)) {
				queryVelocity = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYVOLUME)) {
				queryVolume = Integer.parseInt(value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYENGINE_RELATIONAL)) {
				queryRunningEngine.put(Constants.RELATIONAL, value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYENGINE_GRAPH)) {
				queryRunningEngine.put(Constants.GRAPH, value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYENGINE_NESTED)) {
				queryRunningEngine.put(Constants.NESTED, value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_QUERYENGINE_TEXT)) {
				queryRunningEngine.put(Constants.TEXT, value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_APP_DOMAIN)) {
				app_domain = value;
			}
		}
		
	}
	
}
