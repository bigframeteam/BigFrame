package bigframe.bigif;

/**
 * Class for recording the BigFrame input specification.
 * 
 * @author andy
 * 
 */
public class BigFrameInputFormat {

	private BigDataInputFormat dataIF;
	private BigQueryInputFormat queryIF;
	private WorkflowInputFormat workflowIF;

	public BigFrameInputFormat() {
		dataIF = new BigDataInputFormat();
		queryIF = new BigQueryInputFormat();
		workflowIF = new WorkflowInputFormat();
	}

	public BigFrameInputFormat(BigDataInputFormat dif, BigQueryInputFormat qif, WorkflowInputFormat wif) {
		dataIF = dif;
		queryIF = qif;
		workflowIF = wif;
	}

	public BigDataInputFormat getBigDataInputFormat() {
		return dataIF;
	}

	public BigQueryInputFormat getBigQueryInputFormat() {
		return queryIF;
	}

	public WorkflowInputFormat getWorkflowInputFormat() {
		return workflowIF;
	}
	
	public void setBigDataInputFormat(BigDataInputFormat dif) {
		dataIF = dif;
	}

	public void setBigQueryInputFormat(BigQueryInputFormat qif) {
		queryIF = qif;
	}

	public void setWorkflowInputFormat(WorkflowInputFormat wif) {
		workflowIF = wif;
	}
	
	public void printConf() {
		System.out.println("Benchmark specification:");
		System.out.println("-------------------------------------");
		dataIF.printConf();
		System.out.println("-------------------------------------");
		queryIF.printConf();
		System.out.println("-------------------------------------");
		workflowIF.printConf();
	}
}
