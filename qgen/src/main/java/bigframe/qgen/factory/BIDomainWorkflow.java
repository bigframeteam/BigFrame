package bigframe.qgen.factory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigFrameInputFormat;
import bigframe.qgen.engineDriver.HadoopEngineDriver;
import bigframe.qgen.engineDriver.HiveEngineDriver;
import bigframe.qgen.engineDriver.SharkEngineDriver;
import bigframe.qgen.engineDriver.SparkEngineDriver;
import bigframe.qgen.engineDriver.EngineDriver;
import bigframe.util.Constants;
import bigframe.workflows.BaseTablePath;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * Encapsulate all information of the workflow of a BI domain.
 * 
 * @author andy
 *
 */
public class BIDomainWorkflow extends DomainWorkflow {

	private static final Log LOG = LogFactory.getLog(BIDomainWorkflow.class);
	
	public BIDomainWorkflow(BigFrameInputFormat bigframeIF) {
		super(bigframeIF);
		
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
		
		graph_supportedEngine.add(Constants.HADOOP);
		nested_supportedEngine.add(Constants.HADOOP);
		text_supportedEngine.add(Constants.HADOOP);		
		
		querySupportEngine.put(Constants.RELATIONAL, relational_supportedEngine);
		querySupportEngine.put(Constants.GRAPH, graph_supportedEngine);
		querySupportEngine.put(Constants.NESTED, nested_supportedEngine);
		querySupportEngine.put(Constants.TEXT, text_supportedEngine);
	}

	@Override
	public List<EngineDriver> getWorkflows() {
		/**
		 * Check if all configurations are valid.
		 */
		if(!isBigIFvalid())
			return null;
		
		List<EngineDriver> workflows = new ArrayList<EngineDriver>();
		
		Set<String> dataVariety = bigdataIF.getDataVariety();
		
		String queryVariety = bigqueryIF.getQueryVariety();
		String queryVelocity = bigqueryIF.getQueryVelocity();
		
		/**
		 * Check which engine the user specified for running each part of the query.
		 */
		Map<String, String> queryRunningEngine = bigqueryIF.getQueryRunningEngine();
		String relationalEngine = queryRunningEngine.get(Constants.RELATIONAL);
		String graphEngine = queryRunningEngine.get(Constants.GRAPH);
		String nestedEngine = queryRunningEngine.get(Constants.NESTED);
		
		/**
		 * The currently supported engines
		 */
		HiveEngineDriver hiveWorkflow = new HiveEngineDriver(workflowIF);
		HadoopEngineDriver hadoopWorkflow = new HadoopEngineDriver(workflowIF);
		SharkEngineDriver sharkWorkflow = new SharkEngineDriver(workflowIF);
		SparkEngineDriver sparkWorkflow = new SparkEngineDriver(workflowIF);

		
		/**
		 * Record the paths for all the base table used. 
		 */
		BaseTablePath basePath = new BaseTablePath();
		
		/**
		 * initialize the path for each data set.
		 */
		
		// Remove extra "/" in the end 
		String HADOOP_ROOT_DIR = workflowIF.getHDFSRootDIR();
		String relational_path = HADOOP_ROOT_DIR+"/"+bigdataIF.getDataStoredPath()
				.get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL);
		String graph_path = HADOOP_ROOT_DIR+"/"+bigdataIF.getDataStoredPath()
				.get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_GRAPH);
		String nested_path = HADOOP_ROOT_DIR+"/"+bigdataIF.getDataStoredPath()
				.get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_NESTED);
		
		basePath.relational_path_$eq(relational_path);
		basePath.graph_path_$eq(graph_path);
		basePath.nested_path_$eq(nested_path);
		
		/**
		 * Collect the set of micro queries
		 */
		if(queryVariety.equals(Constants.MICRO)) {
			
			if(queryVelocity.equals(Constants.CONTINUOUS)) {
				
				if(dataVariety.contains(Constants.RELATIONAL)) {
					
					if(relationalEngine.equals(Constants.HADOOP)) {
						
					}
					
				}
				
				else if(dataVariety.contains(Constants.GRAPH)) {
					
					if(graphEngine.equals(Constants.HADOOP)) {
						
					}
				}
				
				else if(dataVariety.contains(Constants.NESTED)) {
					
					if(nestedEngine.equals(Constants.HADOOP)) {
						
					}
					
				}
			}
			
			else if(queryVelocity.equals(Constants.EXPLORATORY)) {
				
				if(dataVariety.contains(Constants.RELATIONAL)) {
					
					if(relationalEngine.equals(Constants.HIVE)) {
						hiveWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSales(basePath));
					}
					else if(relationalEngine.equals(Constants.SHARK)) {
						sharkWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSales(basePath));
					}
					else if(relationalEngine.equals(Constants.HADOOP)) {
						hadoopWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSales(basePath)); 
					}
				}
				
				else if(dataVariety.contains(Constants.GRAPH)) {
					
					if(graphEngine.equals(Constants.HADOOP)) {
						hadoopWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.graph.exploratory.WF_PageRank(basePath.graph_path())); 
					}
				}
				
				else if(dataVariety.contains(Constants.NESTED)) {
					
					if(nestedEngine.equals(Constants.HADOOP)) {
						hadoopWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyze(basePath));
					}
				}
			}
		}
		
		/**
		 * Collect the set of macro queries
		 */
		else if(queryVariety.equals(Constants.MACRO)) {
			
			if(queryVelocity.equals(Constants.CONTINUOUS)) {
				
				
			}
			
			else if(queryVelocity.equals(Constants.EXPLORATORY)) {
				if(relationalEngine.equals(Constants.HADOOP) && graphEngine.equals(Constants.HADOOP)&& 
						nestedEngine.equals(Constants.HADOOP)) {
					hadoopWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentiment(basePath));
				}
			}
		}
		
		// Check if we have queries to run
		if(hiveWorkflow.numOfQueries() > 0)
			workflows.add(hiveWorkflow);
		if(hadoopWorkflow.numOfQueries() > 0)
			workflows.add(hadoopWorkflow);
		if(sharkWorkflow.numOfQueries() > 0)
			workflows.add(sharkWorkflow);
		if(sparkWorkflow.numOfQueries() > 0)
			workflows.add(sparkWorkflow);
		
		return workflows;
	}

	/**
	 * Responsible for guaranteeing every configuration is valid.
	 * 
	 * @return true if no invalid configuration.
	 */
	@Override
	protected boolean isBigIFvalid() {
		// TODO Auto-generated method stub
		// Need to do a lot of sanity tests, including:
		
		if (workflowIF.getHadoopHome().equals("")) {
			LOG.error("Hadoop Home is needed, please set!");
			return false;
		}
		
		
		else if (workflowIF.getHDFSRootDIR().equals("")) {
			LOG.error("HDFS root DIR is needed, please set!");
			return false;
		}
		
		
		else if (!queryVelocity.contains(bigqueryIF.getQueryVelocity())) {
			LOG.error("unsupported query velocity type!");
			LOG.error("Only these are supported:");
			for(String velocity : queryVelocity) {
				System.out.println(velocity);
			}
			return false;
		}
		
		else if (!queryVariety.contains(bigqueryIF.getQueryVariety())) {
			LOG.error("unsupported query velocity type!");
			LOG.error("Only these are supported:");
			for(String velocity : queryVelocity) {
				System.out.println(velocity);
			}
			return false;
		}
		
		else if (!querySupportEngine.get(Constants.RELATIONAL)
				.contains(bigqueryIF.getQueryRunningEngine().get(Constants.RELATIONAL))) {
			LOG.error("unsupported query engine for data type:" + 
					bigqueryIF.getQueryRunningEngine().get(Constants.RELATIONAL));
			LOG.error("Only these are supported:");
			for(String engine : querySupportEngine.get(Constants.RELATIONAL)) {
				System.out.println(engine);
			}
			return false;
		}
		
		else if (!querySupportEngine.get(Constants.GRAPH)
				.contains(bigqueryIF.getQueryRunningEngine().get(Constants.GRAPH))) {
			LOG.error("unsupported query engine for data type:" + 
					bigqueryIF.getQueryRunningEngine().get(Constants.GRAPH));
			LOG.error("Only these are supported:");
			for(String engine : querySupportEngine.get(Constants.GRAPH)) {
				System.out.println(engine);
			}
			return false;
		}
		
		else if (!querySupportEngine.get(Constants.NESTED)
				.contains(bigqueryIF.getQueryRunningEngine().get(Constants.NESTED))) {
			LOG.error("unsupported query engine for data type:" +
					bigqueryIF.getQueryRunningEngine().get(Constants.NESTED));
			LOG.error("Only these are supported:");
			for(String engine : querySupportEngine.get(Constants.NESTED)) {
				System.out.println(engine);
			}
			return false;
		}
		
		else if (!querySupportEngine.get(Constants.TEXT)
				.contains(bigqueryIF.getQueryRunningEngine().get(Constants.TEXT))) {
			LOG.error("unsupported query engine for data type:" + 
					bigqueryIF.getQueryRunningEngine().get(Constants.TEXT));
			LOG.error("Only these are supported:");
			for(String engine : querySupportEngine.get(Constants.TEXT)) {
				System.out.println(engine);
			}
			return false;
		}
		
		for (Entry<String, String> entry : bigqueryIF.getQueryRunningEngine().entrySet()) {
			if(entry.getValue().equals(Constants.HIVE)) {
				if (workflowIF.getHiveHome().equals("")) {
					LOG.error("Hive Home is needed, please set!");
					return false;
				}
				else if (workflowIF.getHiveJDBCServer().equals("")) {
					LOG.error("Hive JDBC Server Address is needed, please set!");
					return false;
				}
			}
		}
		
		return true;
	}

}
