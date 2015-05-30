package bigframe.datagen.nested.tweet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;


import bigframe.datagen.relational.tpcds.CollectTPCDSstat;
import bigframe.datagen.relational.tpcds.CollectTPCDSstatNaive;
import bigframe.datagen.relational.tpcds.TpcdsItemInfo;
import bigframe.datagen.relational.tpcds.TpcdsPromotionInfo;
import bigframe.datagen.text.TextGenFactory;
import bigframe.datagen.text.tweet.TweetTextGen;
import bigframe.datagen.util.RandomUtil;
import bigframe.util.parser.JsonParser;

/**
 * Mapper for generating a specific time range of tweets.
 * It uses @bigframe.datagen.nested.tweet.SimpleTweetGenDist to 
 * generate random tweets.
 * 
 * @author andy
 * 
 */
public class RawTweetMapper extends
Mapper<NullWritable, RawTweetInfoWritable, NullWritable, Text> {
	
	// Get the tweet template file
	private final InputStream TWEET_TEMPLATE_FILE = RawTweetMapper.class.
			getClassLoader().getResourceAsStream("tweet_template.json");
	// private static final Logger LOG = Logger
	// .getLogger(RawTweetGenMapper.class);
	
	@Override
	protected void map(NullWritable ignored,
			RawTweetInfoWritable tweet_gen_info, final Context context)
					throws IOException, InterruptedException {

		long time_begin = tweet_gen_info.begin;
		long time_end = tweet_gen_info.end;
		long tweets_per_mapper = tweet_gen_info.tweets_per_mapper;
		long tweet_start_ID = tweet_gen_info.tweet_start_ID;

		Configuration mapreduce_config = context.getConfiguration();

		int num_twitter_user = mapreduce_config.getInt(
				RawTweetGenConstants.NUM_TWITTER_USER, 0);
		float tpcds_targetGB = mapreduce_config.getFloat(
				RawTweetGenConstants.TPCDS_TARGET_GB, 0);
		float graph_targetGB = mapreduce_config.getFloat(
				RawTweetGenConstants.GRAPH_TARGET_GB, 0);
//		int num_products = mapreduce_config.getInt(
//				RawTweetGenConstants.NUM_PRODUCT, 0);

		CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
		
		TpcdsPromotionInfo promt_info = new TpcdsPromotionInfo();
		TpcdsItemInfo item_info = new TpcdsItemInfo();
		Path[] uris = DistributedCache.getLocalCacheFiles(mapreduce_config);
		for(int i = 0; i < uris.length; i++) {
			BufferedReader in = new BufferedReader(new FileReader(uris[i].toString()));
			if (uris[i].toString().contains(RawTweetGenConstants.PROMOTION_TBL)) {
				tpcds_stat_collecter.setPromtResult(in, promt_info);
			}
			else if (uris[i].toString().contains(RawTweetGenConstants.ITEM_TBL)) {
				tpcds_stat_collecter.setItemResult(in, item_info);
			}
		}

		
		long[] customer_twitterAcc = tpcds_stat_collecter
				.getCustTwitterAcc(tpcds_targetGB, graph_targetGB);
		long[] non_customer_acc = tpcds_stat_collecter
				.getNonCustTwitterAcc(customer_twitterAcc, num_twitter_user);

		JSONObject tweet_json = JsonParser.parseJsonFromFile(TWEET_TEMPLATE_FILE);

		String textgen_name = mapreduce_config.get(
				RawTweetGenConstants.TWEETGEN_NAME, "simple");

		TweetTextGen tweet_textGen = TextGenFactory
				.getTextGenByName(textgen_name);

		if (tweet_textGen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		tweet_textGen.setRandomSeed(RandomUtil.SEEDS_TABLE[0] + time_begin);

		double time_step = (time_end - time_begin) * 1.0 / tweets_per_mapper;

		TweetGenDist tweet_gen_dist = new SimpleTweetGenDist(RandomUtil.SEEDS_TABLE[0] 
				+ time_begin, tweet_textGen, tweet_start_ID);
 
		tweet_gen_dist.init(customer_twitterAcc, non_customer_acc, time_begin, 
				time_step, promt_info, item_info, item_info.getProdName().size(), tweet_json);
		
		for(int i = 0; i < tweets_per_mapper; i++) {
			context.write(null, new Text(tweet_gen_dist.getNextTweet().toString()));
		}

	}

}

