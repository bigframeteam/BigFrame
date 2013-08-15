package bigframe.qgen.engineDriver;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.queries.HadoopRunnable;

/**
 * A class to control the workflow running on hadoop system.
 * 
 * @author andy
 *
 */
public class HadoopWorkflow extends Workflow {
	private static final Logger LOG = Logger.getLogger(HadoopWorkflow.class);
	private List<HadoopRunnable> queries = new ArrayList<HadoopRunnable>();
	
	public HadoopWorkflow(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void run() {
		System.out.println("Running Hadoop Query");
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int numOfQueries() {

		return queries.size();
	}

}
