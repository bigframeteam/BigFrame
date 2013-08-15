package bigframe.datagen.relational.tpcds;



import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.relational.RelationalDataGen;

/**
 * An abstract class for tpcds data generator.
 * 
 * @author andy
 *
 */
public abstract class TpcdsDataGen extends RelationalDataGen {
	
	
	public TpcdsDataGen(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
	
	}

}
