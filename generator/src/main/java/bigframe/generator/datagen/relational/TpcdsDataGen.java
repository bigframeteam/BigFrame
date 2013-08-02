package bigframe.generator.datagen.relational;



import bigframe.generator.datagen.DatagenConf;


public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
