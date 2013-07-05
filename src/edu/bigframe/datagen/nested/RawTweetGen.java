package edu.bigframe.datagen.nested;

import java.io.InputStream;

import edu.bigframe.BigFrameDriver;
import edu.bigframe.datagen.DatagenConf;
import edu.bigframe.datagen.text.TweetTextGen;
import edu.bigframe.datagen.text.TweetTextGenSimple;
import edu.bigframe.util.Constants;

public abstract class RawTweetGen extends NestedDataGen {
	protected InputStream tweet_template_file;
	protected TweetTextGen textgen;
	
	protected String hdfs_dir;
	private final int single_tweet_inByte = 1379;
	
	public RawTweetGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		
		// TODO Auto-generated constructor stub
		tweet_template_file = BigFrameDriver.class.getClassLoader().getResourceAsStream("tweet_template.json");
		textgen = new TweetTextGenSimple(conf, 0);
		
		hdfs_dir = conf.getDataStoredPath().get(Constants.BIGFRAME_DATA_HDFSPATH_NESTED);
		
	}
	
	public long getTweetsPerDay(int days_between) {
		long tweets_per_day = 0;
		
		
		
		long targetByte = (long) (targetGB*1024*1024*1024);
		
		tweets_per_day = (long) (targetByte*1.0/days_between/single_tweet_inByte);
		
		if(tweets_per_day <=0) {
			System.out.println("Tweets sent per day is less than 0, please increase the data volumn " +
						"or increase the proportion of nested data!");
			System.exit(-1);
		}
		
		return tweets_per_day;
	}
	
	public void setTextGen(TweetTextGen tg) {
		textgen = tg;
	}

}
