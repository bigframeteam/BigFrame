package bigframe.workflow.descriptor;



public abstract class Workflow {
    protected WorkflowInputFormat conf;
    protected String driver;
	
	public Workflow(WorkflowInputFormat conf) {
		this.conf = conf;
	}
	
	public abstract void init();
	
	public abstract void run();
	
	
	public abstract void cleanup();
}
