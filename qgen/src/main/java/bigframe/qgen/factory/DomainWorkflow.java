package bigframe.qgen.factory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bigframe.bigif.BigDataInputFormat;
import bigframe.bigif.BigFrameInputFormat;
import bigframe.bigif.BigQueryInputFormat;
import bigframe.bigif.WorkflowInputFormat;
import bigframe.bigif.appDomainInfo.DomainInfo;
import bigframe.qgen.engineDriver.EngineDriver;

/**
 * A class encapsulates workflow information of a specific application domain.
 * Info like:
 * 1. query variety supported
 * 2. query velocity supported
 * 3. query engine supported
 * 4. and the set of queries
 * 
 * To implement the set of queries for a new application, developer needs to first
 * create a new concrete class for that applcaiton by inherit this abstract class.
 *  
 * @author andy
 *
 */
public abstract class DomainWorkflow {
//	protected Set<String> queryVariety = new HashSet<String>();
//	protected Set<String> queryVelocity = new HashSet<String>();
//	protected Map<String, Set<String> > querySupportEngine = new HashMap<String, Set<String> >();
	
	
	protected BigDataInputFormat bigdataIF;
	protected BigQueryInputFormat bigqueryIF;
	protected WorkflowInputFormat workflowIF;
	
	protected DomainInfo domainInfo;
	
	public DomainWorkflow(BigFrameInputFormat bigframeIF) {
		bigdataIF = bigframeIF.getBigDataInputFormat();
		bigqueryIF = bigframeIF.getBigQueryInputFormat();
		workflowIF = bigframeIF.getWorkflowInputFormat();
	}
	
//	public boolean containQueryVariety(String variety) {
//		return queryVariety.contains(variety);
//	}
//	
//	public boolean containQueryVelocity(String velocity) {
//		return queryVelocity.contains(velocity);
//	}
//	
//	public boolean supportEngine(String dataType, String engine) {
//		return querySupportEngine.get(dataType).contains(engine);
//	}
	
	public abstract List<EngineDriver> getWorkflows();
	
	protected abstract boolean isBigIFvalid();
}
