package bigframe.datagen.text;

import bigframe.datagen.DatagenConf;

/**
 * Abstract class for all tweet text generator.
 * 
 * @author andy
 * 
 */
public abstract class TweetTextGen extends TextGen {

	public TweetTextGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

	public abstract void setRandomSeed(long seed);

	public abstract String getNextTweet(int product_id);
}