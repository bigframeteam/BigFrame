package edu.bigframe.datagen.relational;



import edu.bigframe.datagen.DataGenerator;
import edu.bigframe.datagen.DatagenConf;


public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
