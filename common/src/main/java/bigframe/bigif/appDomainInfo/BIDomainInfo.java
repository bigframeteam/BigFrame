package bigframe.bigif.appDomainInfo;

import java.util.HashSet;
import java.util.Set;

import bigframe.util.Constants;

public class BIDomainInfo extends DomainInfo {

	public BIDomainInfo() {
		
		queryVelocity.add(Constants.CONTINUOUS);
		queryVelocity.add(Constants.EXPLORATORY);
		
		queryVariety.add(Constants.MICRO);
		queryVariety.add(Constants.MACRO);
		
		// The engines currently supported for each type of query
		Set<String> relational_supportedEngine = new HashSet<String>();
		Set<String> graph_supportedEngine = new HashSet<String>();
		Set<String> nested_supportedEngine = new HashSet<String>();
		Set<String> text_supportedEngine = new HashSet<String>();
		
		relational_supportedEngine.add(Constants.HIVE);
		relational_supportedEngine.add(Constants.SHARK);
		relational_supportedEngine.add(Constants.HADOOP);
		relational_supportedEngine.add(Constants.VERTICA);
		relational_supportedEngine.add(Constants.SPARK);

		graph_supportedEngine.add(Constants.HIVE);
		graph_supportedEngine.add(Constants.HADOOP);
		graph_supportedEngine.add(Constants.VERTICA);
		graph_supportedEngine.add(Constants.SPARK);
		graph_supportedEngine.add(Constants.SHARK);
		graph_supportedEngine.add(Constants.GIRAPH);
		
		nested_supportedEngine.add(Constants.HIVE);
		nested_supportedEngine.add(Constants.HADOOP);
		nested_supportedEngine.add(Constants.VERTICA);
		nested_supportedEngine.add(Constants.SPARK);
		nested_supportedEngine.add(Constants.SHARK);
		
		text_supportedEngine.add(Constants.HIVE);
		text_supportedEngine.add(Constants.HADOOP);		
		text_supportedEngine.add(Constants.VERTICA);		
		text_supportedEngine.add(Constants.SPARK);	
		text_supportedEngine.add(Constants.SHARK);	

		querySupportEngine.put(Constants.RELATIONAL, relational_supportedEngine);
		querySupportEngine.put(Constants.GRAPH, graph_supportedEngine);
		querySupportEngine.put(Constants.NESTED, nested_supportedEngine);
		querySupportEngine.put(Constants.TEXT, text_supportedEngine);
		
	}

}
