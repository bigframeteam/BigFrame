package bigframe.workflow;

import java.util.ArrayList;
import java.util.List;

import bigframe.bigif.BigFrameInputFormat;
import bigframe.workflow.descriptor.Workflow;


public class WorkflowFactory {
	private BigFrameInputFormat conf;
	
	public WorkflowFactory(BigFrameInputFormat conf) {
		this.conf = conf;
	}
	
	public List<Workflow> createGenerators() {
		List<Workflow> workflows = new ArrayList<Workflow>();
	
		return workflows;
	}
}
