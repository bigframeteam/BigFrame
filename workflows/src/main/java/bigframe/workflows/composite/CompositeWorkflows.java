package bigframe.workflows.composite;

import java.util.ArrayList;
import java.util.List;

import bigframe.bigif.BigFrameInputFormat;
import bigframe.workflows.Workflows;

public class CompositeWorkflows extends Workflows {

	private List<Workflows> workflows;
	
	public CompositeWorkflows(BigFrameInputFormat conf) {
		super(conf);
		// TODO Auto-generated constructor stub
		
		workflows = new ArrayList<Workflows>();
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void profile() {
		// TODO Auto-generated method stub

	}

	@Override
	public void verify() {
		// TODO Auto-generated method stub

	}

	@Override
	public void collectResult() {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
