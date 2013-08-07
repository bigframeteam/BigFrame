package bigframe.workflow;

import java.util.ArrayList;
import java.util.List;

public abstract class Query {
	private List<String> dataTypes;
	private String queryType;
	private String description;
	
	public Query() {
		dataTypes = new ArrayList<String>();
		
		
	}
	
	public void setDataTypes(List<String> dataTypes) {
		this.dataTypes = dataTypes;
	}
	
	public List<String> getDataTypes() {
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
}
