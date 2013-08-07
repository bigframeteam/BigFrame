package bigframe.AppDomain.BusinessIntelligence;

import java.util.HashSet;
import java.util.Set;

import bigframe.AppDomain.AppDomain;

public class BIDomain extends AppDomain{

	public BIDomain() {
		super();
		name = "BI";
		
		dataVariety.add("Relational");
		dataVariety.add("Graph");
		dataVariety.add("Nested");
		dataVariety.add("Text");
		
		queryVariety.add("continuous");
		queryVariety.add("exploratory");
		
		Set<String> relational_supportedEngine = new HashSet<String>();
		Set<String> graph_supportedEngine = new HashSet<String>();
		Set<String> nested_supportedEngine = new HashSet<String>();
		Set<String> text_supportedEngine = new HashSet<String>();
		
		relational_supportedEngine.add("hadoop");
		graph_supportedEngine.add("hadoop");
		nested_supportedEngine.add("hadoop");
		text_supportedEngine.add("hadoop");
		
		querySupportEngine.put("relational", relational_supportedEngine);
		querySupportEngine.put("graph", graph_supportedEngine);
		querySupportEngine.put("nested", nested_supportedEngine);
		querySupportEngine.put("text", text_supportedEngine);
		
	}
}
