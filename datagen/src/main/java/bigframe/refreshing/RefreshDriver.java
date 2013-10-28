package bigframe.refreshing;

import bigframe.bigif.BigDataInputFormat;

public abstract class RefreshDriver {
	protected BigDataInputFormat bigdataIF;
	
	public RefreshDriver(BigDataInputFormat bigdataIF) {
		this.bigdataIF = bigdataIF;
	}
	
	public abstract void prepare();
	
	public abstract void refresh();
}
