package bigframe.datagen.nested;

import java.util.Random;

import org.json.simple.JSONObject;

import bigframe.datagen.relational.PromotionInfo;
import bigframe.datagen.text.TweetTextGen;

public abstract class TweetGenDist extends NestedGenDist {

	protected Random random;
	protected TweetTextGen text_gen;
	protected long tweet_startID;
	
	protected long[] cust_twitter_acc;
	protected long[] noncust_twitter_acc;
	protected long time_begin;
	protected long time_end;
	protected PromotionInfo promt_prods;
	protected int totalnum_prods;
	protected JSONObject tweet_json;
	
	public TweetGenDist(long random_seed, TweetTextGen text_gen, long ID) {
		random = new Random(random_seed);
		this.text_gen = text_gen;
		tweet_startID = ID;		
		
	}
	
	public void init(long[] cust_acc, long[] noncust_acc, long time_begin, long time_end, 
			PromotionInfo promt_prods, int totalnum_prods, JSONObject tweet_json) {
		this.cust_twitter_acc = cust_acc;
		this.noncust_twitter_acc = noncust_acc;
		this.time_begin = time_begin;
		this.time_end = time_end;
		this.promt_prods = promt_prods;
		this.totalnum_prods = totalnum_prods;
		this.tweet_json = tweet_json;
		
	}
	
	public abstract String getNextTweet();
}
