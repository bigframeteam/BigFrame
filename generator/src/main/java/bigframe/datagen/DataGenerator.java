package bigframe.datagen;

import org.apache.log4j.Logger;

/**
 * The abstract class for data generator.
 * 
 * @author andy
 * 
 */
public abstract class DataGenerator {
	private static final Logger LOG = Logger.getLogger(DataGenerator.class);

	protected DatagenConf conf;
	protected float targetGB;

	public DataGenerator(DatagenConf conf, float targetGB) {
		this.conf = conf;
		this.targetGB = targetGB;
	}

	public void setConf(DatagenConf conf) {
		this.conf = conf;
	}



	public DatagenConf getConf() {
		return this.conf;
	}

	public abstract void generate();

	public abstract int getAbsSizeBySF(int sf);

	public abstract int getSFbyAbsSize(int absSize);
}
