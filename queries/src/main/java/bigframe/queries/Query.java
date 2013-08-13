package bigframe.queries;

import java.util.HashSet;
import java.util.Set;

/**
 * The class provide basic information of a query.
 * 
 * @author andy
 *
 */
public abstract class Query {
	protected Set<String> dataTypes;
	protected String queryType;
	protected String description;
	protected String runningEngine;
	
	public Query() {
		dataTypes = new HashSet<String>();
			
	}
	
	public void setDataTypes(Set<String> dataTypes) {
		this.dataTypes = dataTypes;
	}
	
	public Set<String> getDataTypes() {
		return dataTypes;
	}
	
	public String getqueryType() {
		return queryType;
	}
	
	public void setqueryType(String queryType) {
		this.queryType = queryType;
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String des) {
		description = des;
	}
	
	public abstract void printDescription();
	
}
