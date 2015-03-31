package bigframe.qgen.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigFrameInputFormat;
import bigframe.bigif.appDomainInfo.BIDomainInfo;
import bigframe.qgen.engineDriver.BagelEngineDriver;
import bigframe.qgen.engineDriver.HadoopEngineDriver;
import bigframe.qgen.engineDriver.HiveEngineDriver;
import bigframe.qgen.engineDriver.ImpalaEngineDriver;
import bigframe.qgen.engineDriver.ImpalaHiveEngineDriver;
import bigframe.qgen.engineDriver.SharkBagelEngineDriver;
import bigframe.qgen.engineDriver.SharkEngineDriver;
import bigframe.qgen.engineDriver.SparkEngineDriver;
import bigframe.qgen.engineDriver.SparkSQLEngineDriver;
import bigframe.qgen.engineDriver.EngineDriver;

import bigframe.qgen.engineDriver.VerticaEngineDriver;
import bigframe.util.Constants;
import bigframe.workflows.BaseTablePath;
import bigframe.workflows.util.HDFSUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Encapsulate all information of the workflow for the BI domain.
 * 
 * @author andy
 *
 */
public class BIDomainWorkflow extends DomainWorkflow {

	private static final Log LOG = LogFactory.getLog(BIDomainWorkflow.class);
	
	public BIDomainWorkflow(BigFrameInputFormat bigframeIF) {
		super(bigframeIF);
		domainInfo = new BIDomainInfo();
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
		BagelEngineDriver bagelWorkflow = new BagelEngineDriver(workflowIF);
		SharkBagelEngineDriver sharkbagelWorkflow = new SharkBagelEngineDriver(workflowIF);
		SparkEngineDriver sparkWorkflow = new SparkEngineDriver(workflowIF);
		VerticaEngineDriver verticaWorkflow = new VerticaEngineDriver(workflowIF);
		GiraphEngineDriver giraphWorkflow = new GiraphEngineDriver(workflowIF);
		HiveGiraphEngineDriver hivegiraphWorkflow = new HiveGiraphEngineDriver(workflowIF);
		SparkSQLEngineDriver sparkSQLWorkflow = new SparkSQLEngineDriver(workflowIF);
	
		ImpalaEngineDriver impalaWorkflow = new ImpalaEngineDriver(workflowIF);
		ImpalaHiveEngineDriver impalahiveWorkflow = new ImpalaHiveEngineDriver(workflowIF);
		
		/**
		 * Record the paths for all the base table used. 
		 */
		BaseTablePath basePath = new BaseTablePath();
		
		/**
		 * initialize the path for each data set.
		 */
		
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
					else if(relationalEngine.equals(Constants.VERTICA)) {
						verticaWorkflow.addQuery(new
								bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSales(basePath)); 
					}
					else if(relationalEngine.equals(Constants.SPARK)) {
					//	sparkWorkflow.addQuery(new 
					//			bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSalesSpark(basePath)); 
					}
				}
				
				else if(dataVariety.contains(Constants.GRAPH)) {					
					
					
					if(graphEngine.equals(Constants.BAGEL)) {
						
						String randandsuffvec = workflowIF.getHiveWareHouse() + "/randandsuffvec";
						String transitMatrix = workflowIF.getHiveWareHouse() + "/transitmatrix";
						int numItr = 10;
						double alpha = 0.85;
						int numPartition = workflowIF.getSparkDoP();
						String output_dir = "twitterRank_Bagel";
						
						HDFSUtil hdfsUtil = new HDFSUtil(workflowIF.getHadoopHome());
						
						hdfsUtil.deleteFile(output_dir);
						output_dir = workflowIF.getHDFSRootDIR() + "/" + output_dir;
						bagelWorkflow.addQuery(new
								bigframe.workflows.BusinessIntelligence.graph.exploratory.WF_TwitterRankBagel(
										randandsuffvec, transitMatrix, numItr, alpha, numPartition, output_dir));
					}
				}
				
				else if(dataVariety.contains(Constants.NESTED)) {
					
					if(nestedEngine.equals(Constants.HADOOP)) {
						hadoopWorkflow.addQuery(new 
								bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyze(basePath));
					}
					else if(nestedEngine.equals(Constants.SPARK)) {
					//	sparkWorkflow.addQuery(new 
					//			bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyzeSpark(basePath, "[aA].*"));
					}
					
					
					else if(nestedEngine.equals(Constants.HIVE)) {
						hiveWorkflow.addQuery(new 
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
				//Relational, Text, Graph for Hadoop
				if(relationalEngine.equals(Constants.HADOOP) && graphEngine.equals(Constants.HADOOP)&& 
						nestedEngine.equals(Constants.HADOOP)) {
					hadoopWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentHadoop(basePath, 10));
				}
				else if(relationalEngine.equals(Constants.VERTICA) && graphEngine.equals(Constants.VERTICA)&& 
						nestedEngine.equals(Constants.VERTICA)) {
					boolean jsonasstring = false;
					if(workflowIF.getProp().containsKey("jsonasstring"))
						jsonasstring = true;
					
					verticaWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentVertica(basePath, 10, jsonasstring));
				}
				
				else if(relationalEngine.equals(Constants.HIVE) && graphEngine.equals(Constants.HIVE)&& 
						nestedEngine.equals(Constants.HIVE)) {
					hiveWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentHive(basePath, 10, workflowIF.getHiveORC()));
				}
				else if(relationalEngine.equals(Constants.SPARKSQL) && graphEngine.equals(Constants.SPARKSQL)&& 
						nestedEngine.equals(Constants.SPARKSQL)) {
					System.out.println("SparkSQL workflow added");
					sparkSQLWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentSparkSQL(basePath, 10, workflowIF.getHiveORC()));
				}
				else if(relationalEngine.equals(Constants.HIVE) && graphEngine.equals(Constants.GIRAPH)&& 
						nestedEngine.equals(Constants.HIVE)) {
					hivegiraphWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentHiveGiraph(basePath, 10, workflowIF.getHiveORC()));
				}
				
/*				else if(relationalEngine.equals(Constants.SHARK) && graphEngine.equals(Constants.SHARK)&& 
						nestedEngine.equals(Constants.SHARK)) {
					sharkWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentiment(basePath, 10));
				}*/
				
				
				else if(relationalEngine.equals(Constants.SHARK) && graphEngine.equals(Constants.SHARK)&& 
						nestedEngine.equals(Constants.SHARK)) {
					sharkWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentShark(basePath, 10, workflowIF.getSharkRC()));
				}
				
				else if(relationalEngine.equals(Constants.SHARK) && graphEngine.equals(Constants.BAGEL)&& 
						nestedEngine.equals(Constants.SHARK)) {
					sharkbagelWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentSharkBagel(basePath, 
									workflowIF.getHadoopHome(), workflowIF.getHiveWareHouse(), 
									10, workflowIF.getSharkRC(), workflowIF.getSparkDoP(), 
									workflowIF.getSharkSnappy()));
				}
				
				//Relational, Text, Graph for Spark
				else if(relationalEngine.equals(Constants.SPARK) && graphEngine.equals(Constants.SPARK)&& 
						nestedEngine.equals(Constants.SPARK)) {
				//	sparkWorkflow.addQuery(new 
				//			bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_MacroRTGSpark(basePath, 10, workflowIF.getSparkUseBagel(), workflowIF.getSparkDoP(), workflowIF.getSparkOptimizeMemory()));
				} 
					//sparkWorkflow.addQuery(new 
						//	bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_MacroRTGSpark(basePath, 10, workflowIF.getSparkUseBagel(), workflowIF.getSparkDoP(), workflowIF.getSparkOptimizeMemory()));
					sparkWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentSpark(basePath, 
									workflowIF.getHadoopHome(), 10, 0.85, workflowIF.getSparkDoP(), true));
				}
				//Relational, Text for Spark
				else if(nestedEngine.equals(Constants.SPARK) && nestedEngine.equals(Constants.SPARK)) {
				//	sparkWorkflow.addQuery(new 
				//			bigframe.workflows.BusinessIntelligence.RT.exploratory.WF_PromotionAnalyzeSpark(basePath, workflowIF.getSparkDoP()));
				}
				
				//Relational, Text for Spark
				else if(relationalEngine.equals(Constants.IMPALA) && graphEngine.equals(Constants.IMPALA)&& 
						nestedEngine.equals(Constants.IMPALA)) {
					impalaWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentImpala(basePath));
				}
				
				else if(relationalEngine.equals(Constants.IMPALA) && graphEngine.equals(Constants.IMPALA)&& 
						nestedEngine.equals(Constants.HIVE)) {
					impalahiveWorkflow.addQuery(new 
							bigframe.workflows.BusinessIntelligence.RTG.exploratory.WF_ReportSaleSentimentImpalaHive(basePath, 10, 
									workflowIF.getHiveORC(), workflowIF.getSharkSnappy(), workflowIF.getImpalaHiveFileFormat()));
				}
			}
		}
		
		// Check if we have queries to run
		if(hiveWorkflow.numOfQueries() > 0)
			workflows.add(hiveWorkflow);
		if(sparkSQLWorkflow.numOfQueries() > 0)
			workflows.add(sparkSQLWorkflow);
		if(hadoopWorkflow.numOfQueries() > 0)
			workflows.add(hadoopWorkflow);
		if(sharkWorkflow.numOfQueries() > 0)
			workflows.add(sharkWorkflow);
		if(bagelWorkflow.numOfQueries() > 0)
			workflows.add(bagelWorkflow);
		if(sharkbagelWorkflow.numOfQueries() > 0)
			workflows.add(sharkbagelWorkflow);
		if((Integer)sparkWorkflow.numOfQueries() > 0)
			workflows.add(sparkWorkflow);
		if((Integer)impalaWorkflow.numOfQueries() > 0)
			workflows.add(impalaWorkflow);
		if((Integer)impalahiveWorkflow.numOfQueries() > 0)
			workflows.add(impalahiveWorkflow);
		if(verticaWorkflow.numOfQueries() > 0)
			workflows.add(verticaWorkflow);
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
			
		Set<String> queryVariety = domainInfo.getQueryVariety();
		Set<String> queryVelocity = domainInfo.getQueryVelocity();
		Map<String, Set<String> > querySupportEngine = domainInfo.getQuerySupportEngine();
		
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
		
		for (Entry<String, String> entry : bigqueryIF.getQueryRunningEngine().entrySet()) {
			if(entry.getValue().equals(Constants.VERTICA)) {
				if (workflowIF.getVerticaHome().equals("")) {
					LOG.error("VERTICA Home is needed, please set!");
					return false;
				}
				else if (workflowIF.getVerticaJDBCServer().equals("")) {
					LOG.error("Vertica JDBC Server Address is needed, please set!");
					return false;
				}
			}
		}
		
		return true;
	}

}
