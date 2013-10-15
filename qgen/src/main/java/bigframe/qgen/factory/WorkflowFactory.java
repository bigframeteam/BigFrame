package bigframe.qgen.factory;


import java.util.List;


import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.bigif.BigFrameInputFormat;
import bigframe.qgen.engineDriver.EngineDriver;

/**
 * A factory to create the set of workflows the user requires.
 * 
 * @author andy
 *
 */
public class WorkflowFactory {
	private BigFrameInputFormat bigframeIF;
	
	public WorkflowFactory(BigFrameInputFormat conf) {
		this.bigframeIF = conf;
	}
	
	public List<EngineDriver> createWorkflows() {
	
		BigDataInputFormat dataIF = bigframeIF.getBigDataInputFormat();
		
		DomainWorkflow workflowInfo;
		
		String app_domain = dataIF.getAppDomain();

		if (app_domain.equals(BigConfConstants.APPLICATION_BI)) {
			workflowInfo = new BIDomainWorkflow(bigframeIF);
		}
		else 
			return null;
		
		return workflowInfo.getWorkflows();
	}
}
