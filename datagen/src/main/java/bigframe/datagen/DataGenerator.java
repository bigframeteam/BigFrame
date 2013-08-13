package bigframe.datagen;

import bigframe.bigif.BigDataInputFormat;

/**
 * The abstract class for data generator.
 * 
 * @author andy
 * 
 */
public abstract class DataGenerator {
	//private static final Logger LOG = Logger.getLogger(DataGenerator.class);

	protected BigDataInputFormat conf;
	protected float targetGB;

	public DataGenerator(BigDataInputFormat conf, float targetGB) {
		this.conf = conf;
		this.targetGB = targetGB;
	}

	public void setConf(BigDataInputFormat conf) {
		this.conf = conf;
	}



	public BigDataInputFormat getConf() {
		return this.conf;
	}

	public abstract void generate();

	public abstract int getAbsSizeBySF(int sf);

	public abstract int getSFbyAbsSize(int absSize);
}
