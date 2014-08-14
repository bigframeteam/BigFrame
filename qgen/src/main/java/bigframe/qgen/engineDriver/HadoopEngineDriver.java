package bigframe.qgen.engineDriver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.events.BigFrameListenerBus;
import bigframe.workflows.runnable.HadoopRunnable;


/**
 * A class to control the workflow running on hadoop system.
 * 
 * @author andy
 *
 */
public class HadoopEngineDriver extends EngineDriver {
	private Configuration mapred_config;
	private static final Logger LOG = Logger.getLogger(HadoopEngineDriver.class);
	private List<HadoopRunnable> queries = new ArrayList<HadoopRunnable>();
	
	public HadoopEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub
		mapred_config = new Configuration();
		mapred_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/core-site.xml"));
		mapred_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/mapred-site.xml"));
	}

	@Override
	public void run(BigFrameListenerBus eventBus) {
		System.out.println("Running Hadoop Query");
		for(HadoopRunnable query : queries) {
			init();
			if(query.runHadoop(mapred_config))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int numOfQueries() {

		return queries.size();
	}

	public void addQuery(HadoopRunnable q) {
		// TODO Auto-generated method stub
		queries.add(q);
	}

}
