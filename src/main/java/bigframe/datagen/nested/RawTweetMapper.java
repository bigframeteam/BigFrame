package bigframe.datagen.nested;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;

import bigframe.datagen.relational.CollectTPCDSstat;
import bigframe.datagen.relational.CollectTPCDSstatNaive;
import bigframe.datagen.relational.PromotionInfo;
import bigframe.datagen.text.TextGenFactory;
import bigframe.datagen.text.TweetTextGen;
import bigframe.util.RandomSeeds;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;

/**
 * Mapper for generating a specific time range of tweets.
 * 
 * @author andy
 * 
 */
public class RawTweetMapper extends
Mapper<NullWritable, RawTweetInfoWritable, NullWritable, Text> {

	// private static final Logger LOG = Logger
	// .getLogger(RawTweetGenMapper.class);

	@SuppressWarnings("unchecked")
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
		int num_products = mapreduce_config.getInt(
				RawTweetGenConstants.NUM_PRODUCT, 0);

		CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
		PromotionInfo promt_info = new PromotionInfo();
		tpcds_stat_collecter.collectHDFSPromtResult(mapreduce_config,
				RawTweetGenConstants.PROMOTION_TBL, promt_info);


		long[] customer_twitterAcc = tpcds_stat_collecter
				.getCustTwitterAcc(tpcds_targetGB, graph_targetGB);
		long[] non_customer_acc = tpcds_stat_collecter
				.getNonCustTwitterAcc(customer_twitterAcc, num_twitter_user);

		JSONObject tweet_json = RawTweetGenConstants.TWEET_JSON;

		String textgen_name = mapreduce_config.get(
				RawTweetGenConstants.TWEETGEN_NAME, "simple");

		TweetTextGen tweet_textGen = TextGenFactory
				.getTextGenByName(textgen_name);

		if (tweet_textGen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		tweet_textGen.setRandomSeed(RandomSeeds.SEEDS_TABLE[0] + time_begin);


		TweetGenDist tweet_gen_dist = new SimpleTweetGenDist(RandomSeeds.SEEDS_TABLE[0] 
				+ time_begin, tweet_textGen, tweet_start_ID);
		// The conversion from int to long for time_begin and time_end will lost precision. 
		tweet_gen_dist.init(customer_twitterAcc, non_customer_acc, time_begin, 
				time_end, promt_info, num_products, tweet_json);
		
		for(int i = 0; i < tweets_per_mapper; i++) {
			context.write(null, new Text(tweet_gen_dist.getNextTweet().toString()));
		}

	}

}

