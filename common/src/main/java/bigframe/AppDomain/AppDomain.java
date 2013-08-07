package bigframe.AppDomain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AppDomain {
	protected String name;
	protected Set<String> dataVariety;
	protected Set<String> queryVariety;
	protected Set<String> dataVelocity;
	protected Set<String> queryVelocity;
	protected Map<String, Set<String> > querySupportEngine;

	
	public AppDomain() {
		dataVariety = new HashSet<String>();
		dataVelocity = new HashSet<String>();
		queryVariety = new HashSet<String>();
		queryVelocity = new HashSet<String>();
		querySupportEngine = new HashMap<String, Set<String> >();
	}
	
//	public void addDataVariety(String variety) {
//			dataVariety.add(variety);
//	}
//	
//	public void addDataVelocity(String velocity) {
//			dataVelocity.add(velocity);
//	}
//	
//	public void addQueryVariety(String variety) {
//			queryVariety.add(variety);
//	}
//	
//	public void addQueryVelocity(String velocity) {
//			queryVelocity.add(velocity);
//	}
//	
//
//	
//	public String getName() {
//		return name;
//	}
//	
//	public Set<String> getDataVariety() {
//		return dataVariety;
//	}
//	
//	public Set<String> getQueryVariety() {
//		return queryVariety;
//	}
//	
//	public Set<String> getDataVelocity() {
//		return dataVelocity;
//	}
//	
//	public Set<String> getQueryVelocity() {
//		return queryVelocity;
//	}
//	
//	
//	public void setName(String name) {
//		this.name = name;
//	}
//	
//	public void setDataVariety(Set<String> dataVariety) {
//		this.dataVariety = dataVariety;
//	}
//	
//	public void setQueryVariety(Set<String> queryVariety) {
//		this.queryVariety = queryVariety;
//	}
//	
//	public void setDataVelocity(Set<String> dataVelocity) {
//		this.dataVelocity = dataVelocity;
//	}
//	
//	public void setQueryVelocity(Set<String> queryVelocity) {
//		this.queryVelocity = queryVelocity;
//	}
	
	public boolean containDataVariety(String variety) {
		return dataVariety.contains(variety);
	}
	
	public boolean containDataVelocity(String velocity) {
		return dataVelocity.contains(velocity);
	}
	
	public boolean containQueryVariety(String variety) {
		return queryVariety.contains(variety);
	}
	
	public boolean containQueryVelocity(String velocity) {
		return queryVelocity.contains(velocity);
	}
	
	public boolean supportEngine(String dataType, String engine) {
		return querySupportEngine.get(dataType).contains(engine);
	}
}
