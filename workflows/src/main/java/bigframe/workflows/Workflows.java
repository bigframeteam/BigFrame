package bigframe.workflows;

import bigframe.bigif.BigFrameInputFormat;

public abstract class Workflows {
    protected BigFrameInputFormat conf;
	
	public Workflows(BigFrameInputFormat conf) {
		this.conf = conf;
	}
	
	public abstract void setup();
	
	public abstract void run();
	
	public abstract void profile();
	
	public abstract void verify();
	
	public abstract void collectResult();
	
	public abstract void cleanup();
}
