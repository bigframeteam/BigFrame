package bigframe.datagen.text;

import bigframe.datagen.DatagenConf;

public abstract class TweetTextGen extends TextGen {
	
	public TweetTextGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}
	
	public abstract String getNextTweet(int product_id);
}