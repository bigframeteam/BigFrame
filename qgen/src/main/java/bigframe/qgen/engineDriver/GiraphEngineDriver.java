package bigframe.qgen.engineDriver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.events.BigFrameListenerBus;
import bigframe.workflows.runnable.GiraphRunnable;

public class GiraphEngineDriver extends EngineDriver {

	private GiraphConfiguration giraph_config = new GiraphConfiguration();
	private static final Logger LOG = Logger.getLogger(GiraphEngineDriver.class);
	private List<GiraphRunnable> queries = new ArrayList<GiraphRunnable>();
	
	public GiraphEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		Configuration.addDefaultResource("giraph-site.xml");
		giraph_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/core-site.xml"));
		giraph_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/mapred-site.xml"));
	}

	@Override
	public int numOfQueries() {
		return queries.size();
	}

	@Override
	public void init() {

	}

	@Override
	public void run(BigFrameListenerBus eventBus) {
		LOG.info("Running Giraph queries!");
		
		for(GiraphRunnable query : queries) {
			if(query.runGiraph(giraph_config))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}

	}

	public void addQuery(GiraphRunnable query) {
		queries.add(query);
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
