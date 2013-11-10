package bigframe.refreshing;

import bigframe.bigif.BigDataInputFormat;

public abstract class DataPreparator {
	protected BigDataInputFormat bigdataIF;
		
	public DataPreparator(BigDataInputFormat bigdataIF) {
		this.bigdataIF = bigdataIF;
	}

	public abstract void prepare();
}
