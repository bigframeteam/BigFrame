package bigframe.bigif.appDomainInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class DomainInfo {

	protected Set<String> queryVariety = new HashSet<String>();
	protected Set<String> queryVelocity = new HashSet<String>();
	protected Map<String, Set<String> > querySupportEngine = new HashMap<String, Set<String> >();
	
	public DomainInfo() {
		// TODO Auto-generated constructor stub
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
	
	public Set<String> getQueryVariety() {
		return queryVariety;
	}
	
	public Set<String> getQueryVelocity() {
		return queryVelocity;
	}
	
	public Map<String, Set<String> > getQuerySupportEngine() {
		return querySupportEngine;
	}
	
}
