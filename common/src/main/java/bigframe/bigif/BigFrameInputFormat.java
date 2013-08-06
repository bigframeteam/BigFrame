package bigframe.bigif;

/**
 * Class for recording the BigFrame input specification.
 * 
 * @author andy
 * 
 */
public class BigFrameInputFormat {

	private BigDataInputFormat dataInputFormat;
	private BigQueryInputFormat queryInputFormat;


	public BigFrameInputFormat() {
		dataInputFormat = new BigDataInputFormat();
		queryInputFormat = new BigQueryInputFormat();

	}

	public BigFrameInputFormat(BigDataInputFormat dc, BigQueryInputFormat qc) {
		dataInputFormat = dc;
		queryInputFormat = qc;
	}

	public BigDataInputFormat getBigDataInputFormat() {
		return dataInputFormat;
	}

	public BigQueryInputFormat getBigQueryInputFormat() {
		return queryInputFormat;
	}

	public void setBigDataInputFormat(BigDataInputFormat dc) {
		dataInputFormat = dc;
	}

	public void setBigQueryInputFormat(BigQueryInputFormat qc) {
		queryInputFormat = qc;
	}

	public void printConf() {
		System.out.println("Benchmark specification:");
		System.out.println("-------------------------------------");
		dataInputFormat.printConf();
		System.out.println("-------------------------------------");
		queryInputFormat.printConf();
	}
}
