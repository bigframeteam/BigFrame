package bigframe.datagen.nested.tweet;

import java.util.Random;

import org.json.simple.JSONObject;

import bigframe.datagen.nested.NestedGenDist;
import bigframe.datagen.relational.tpcds.TpcdsItemInfo;
import bigframe.datagen.relational.tpcds.TpcdsPromotionInfo;
import bigframe.datagen.text.tweet.TweetTextGen;

/**
 * An abstract class for tweet distribution control.
 * 
 * @author andy
 *
 */
public abstract class TweetGenDist extends NestedGenDist {

	protected Random random;
	protected TweetTextGen text_gen;
	protected long tweet_startID;
	
	protected long[] cust_twitter_acc;
	protected long[] noncust_twitter_acc;
	protected long time_begin;
	protected long time_end;
	protected TpcdsPromotionInfo promt_info;
	protected TpcdsItemInfo item_info;
	protected int totalnum_prods;
	protected JSONObject tweet_json;
	
	public TweetGenDist(long random_seed, TweetTextGen text_gen, long ID) {
		random = new Random(random_seed);
		this.text_gen = text_gen;
		tweet_startID = ID;		
		
	}
	
	public void init(long[] cust_acc, long[] noncust_acc, long time_begin, long time_end, 
			TpcdsPromotionInfo promt_info, TpcdsItemInfo item_info,
			int totalnum_prods, JSONObject tweet_json) {
		this.cust_twitter_acc = cust_acc;
		this.noncust_twitter_acc = noncust_acc;
		this.time_begin = time_begin;
		this.time_end = time_end;
		this.promt_info = promt_info;
		this.item_info = item_info;
		this.totalnum_prods = totalnum_prods;
		this.tweet_json = tweet_json;
		
	}
	
	public abstract String getNextTweet();
}
