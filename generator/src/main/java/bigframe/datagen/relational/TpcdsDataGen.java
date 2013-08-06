package bigframe.datagen.relational;



import bigframe.bigif.BigDataInputFormat;


public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
