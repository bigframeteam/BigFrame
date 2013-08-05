package bigframe.datagen.relational;



import bigframe.datagen.DatagenConf;


public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
