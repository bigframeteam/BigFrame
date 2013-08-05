package bigframe.datagen.relational;



import bigframe.bigif.DatagenConf;


public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
