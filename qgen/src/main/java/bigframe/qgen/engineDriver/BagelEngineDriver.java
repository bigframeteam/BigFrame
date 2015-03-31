package bigframe.qgen.engineDriver;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.runnable.BagelRunnable;

public class BagelEngineDriver extends EngineDriver {

	private SparkContext sc;
	private static final Logger LOG = Logger.getLogger(BagelEngineDriver.class);
	private List<BagelRunnable> queries = new ArrayList<BagelRunnable>();
	
	public BagelEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int numOfQueries() {
		// TODO Auto-generated method stub
		return queries.size();
	}

	public void addQuery(BagelRunnable query) {
		queries.add(query);
	}
	
	@Override
	public void init() {
		// TODO Auto-generated method stub

		/**
		 * Initialize the spark context.
		 */
		
		String sparkMaster = workIF.getSparkMaster();
		String appName = "BigFrame Benchmark";
		String sparkHome = workIF.getSparkHome();
		
		System.setProperty("spark.local.dir", workIF.getSparkLocalDir());
		
		String jar_path_string = System.getenv(BigConfConstants.WORKFLOWS_JAR);
		
		sc = new SparkContext(sparkMaster, appName, sparkHome, null, null, null);
		sc.addJar(jar_path_string);
	}

	@Override
	public void run() {
		LOG.info("Running Bagel Query");
		
		for(BagelRunnable query : queries) {
			if(query.runBagel(sc))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
