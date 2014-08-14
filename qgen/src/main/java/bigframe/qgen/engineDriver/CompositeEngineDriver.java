package bigframe.qgen.engineDriver;

import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.events.BigFrameListenerBus;


/**
 * A class to control the workflow running on multiple systems.
 * 
 * @author andy
 *
 */
public class CompositeEngineDriver extends EngineDriver {

	private static final Logger LOG = Logger.getLogger(CompositeEngineDriver.class);
	
	public CompositeEngineDriver(WorkflowInputFormat conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void run(BigFrameListenerBus eventBus) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int numOfQueries() {
		// TODO Auto-generated method stub
		return 0;
	}

}
