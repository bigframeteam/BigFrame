package bigframe.qgen.engineDriver;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.events.BigFrameListenerBus;


/**
 * A abstract class to control the workflow running on a possible system.
 * 
 * @author andy
 *
 */
public abstract class EngineDriver {
    protected WorkflowInputFormat workIF;
	
	public EngineDriver(WorkflowInputFormat workIF) {
		this.workIF = workIF;
	}
	
	public abstract int numOfQueries();
	
	public abstract void init();
	
	public abstract void run(BigFrameListenerBus eventBus);
	
	public abstract void cleanup();
}
