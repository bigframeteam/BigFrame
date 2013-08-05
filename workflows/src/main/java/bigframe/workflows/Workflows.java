package bigframe.workflows;

import bigframe.bigif.BenchmarkConf;

public abstract class Workflows {
    protected BenchmarkConf conf;
	
	public Workflows(BenchmarkConf conf) {
		this.conf = conf;
	}
	
	public abstract void setup();
	
	public abstract void run();
	
	public abstract void profile();
	
	public abstract void verify();
	
	public abstract void collectResult();
	
	public abstract void cleanup();
}
