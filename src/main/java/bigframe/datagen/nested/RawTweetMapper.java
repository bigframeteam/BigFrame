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
		// LOG.info("Begin time: " + tweet_gen_info.begin + ";" +
		// "End time: "
		// + tweet_gen_info.end + ";" + "Tweets per day: "
		// + tweet_gen_info.tweets_per_day);
		long time_begin = tweet_gen_info.begin;
		long time_end = tweet_gen_info.end;
		long tweets_per_day = tweet_gen_info.tweets_per_day;

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

		/*
		 * Get the set of promoted products and their promotion period.
		 */
		ArrayList<Integer> dateBeginSK = promt_info.getDateBeginSK();
		ArrayList<Integer> dateEndSK = promt_info.getDateEndSK();
		ArrayList<Integer> productSK = promt_info.getProductSK();

		ArrayList<Integer> candidate_promoted_prod = new ArrayList<Integer>();

		for (int i = 0; i < dateBeginSK.size(); i++) {
			if (promt_info.getDateBySK(dateBeginSK.get(i)).getTime() / 1000 > time_end) {
				continue;
			}

			if (promt_info.getDateBySK(dateEndSK.get(i)).getTime() / 1000 < time_begin) {
				continue;
			}

			candidate_promoted_prod.add(productSK.get(i));
		}

		/*
		 * Set the probability for mentioning products
		 */
		ProductMentionProb mention_prob = new ProductMentionProb();

		double cust_mem_prod = mention_prob.getCustMenProb();
		double noncust_mem_prod = mention_prob.getNonCustMenProb();
		double promoted_product_men_prob = mention_prob
				.getPromotedProdMenProbCust();
		double promoted_prod_men_prob_noncust = mention_prob
				.getPromotedProdMenProbNonCust();

		long[] customer_twitterAcc = tpcds_stat_collecter
				.getCustTwitterAcc(tpcds_targetGB, graph_targetGB);
		long[] non_customer_acc = tpcds_stat_collecter
				.getNonCustTwitterAcc(customer_twitterAcc, num_twitter_user);

		int num_cust_acc = customer_twitterAcc.length;
		int num_noncust_acc = non_customer_acc.length;

		assert (num_cust_acc != 0);

		/**
		 * Calculate tweets sent per time unit
		 */

		// Initialise the time unit to be 1 second.
		int timeunit = 1;
		double tweets_per_sec = tweets_per_day * 1.0 / 24 / 3600;

		long tweets_per_timeunit = (long) tweets_per_sec;

		// Compute the time unit such that there will be at least one tweet
		// sent
		// in this time unit
		int exponent = 1;
		while (tweets_per_timeunit == 0) {
			tweets_per_timeunit = (long) (tweets_per_sec * Math.pow(10,
					exponent));
			timeunit = timeunit * 10;
			exponent++;
		}

		// Number of tweets per time unit sent by customer and non-customer
		// respectively
		int tweets_customer_perunit = (int) (tweets_per_timeunit
				* num_cust_acc * 1.0 / num_twitter_user);
		int tweets_noncustomer_perunit = (int) (tweets_per_timeunit
				* num_noncust_acc * 1.0 / num_twitter_user);

		// Compute the time unit such that there will be at least one tweet
		// sent by customer
		// in this time unit
		exponent = 1;
		while (tweets_customer_perunit == 0) {
			tweets_customer_perunit = (int) (tweets_per_timeunit
					* num_cust_acc * 1.0 / num_twitter_user * Math.pow(10,
							exponent));
			tweets_noncustomer_perunit = (int) (tweets_per_timeunit
					* num_noncust_acc * 1.0 / num_twitter_user * Math.pow(
							10, exponent));
			timeunit = timeunit * 10;
			exponent++;
		}

		JSONObject tweet_json = RawTweetGenConstants.TWEET_JSON;
		JSONObject user_json = (JSONObject) tweet_json.get("user");

		SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
		twitterDateFormat.setLenient(false);

		RandomEngine twister = new MersenneTwister(
				(int) (RandomSeeds.SEEDS_TABLE[0] + time_begin));
		Random randnum = new Random(RandomSeeds.SEEDS_TABLE[0] + time_begin);

		String textgen_name = mapreduce_config.get(
				RawTweetGenConstants.TWEETGEN_NAME, "simple");

		TweetTextGen tweet_textGen = TextGenFactory
				.getTextGenByName(textgen_name);

		if (tweet_textGen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		tweet_textGen.setRandomSeed(RandomSeeds.SEEDS_TABLE[0] + time_begin);


		long tweet_id = 0;
		// For each time unit, randomly get a set of people to sent tweets
		for (long timestamp = time_begin; timestamp <= time_end; timestamp += timeunit) {
			long[] selected_cust = new long[tweets_customer_perunit];
			long[] selected_noncust = new long[tweets_noncustomer_perunit];

			RandomSampler.sample(tweets_customer_perunit, num_cust_acc,
					tweets_customer_perunit, 1, selected_cust, 0, twister);
			RandomSampler.sample(tweets_noncustomer_perunit,
					num_noncust_acc, tweets_noncustomer_perunit, 1,
					selected_noncust, 0, twister);

			for (int j = 0; j < tweets_customer_perunit; j++) {
				double flip = randnum.nextDouble();
				// Get a set of customers talk about the product
				if (flip <= cust_mem_prod) {

					int product_id;

					double flip2 = randnum.nextDouble();
					/************************************************************
					 * mention the promoted products
					 ***********************************************************/
					if (flip2 <= promoted_product_men_prob) {

						product_id = candidate_promoted_prod.get(randnum
								.nextInt(candidate_promoted_prod.size()));

					} else {
						product_id = randnum.nextInt(num_products);
					}

					String tweet = tweet_textGen
							.getNextTweet(product_id);
					String date = twitterDateFormat
							.format(timestamp * 1000);

					tweet_json.put("created_at", date);
					tweet_json.put("text", tweet);
					tweet_json.put("id", String.valueOf(tweet_id));

					// How to put nested attribute?

					user_json.put("id", selected_cust[j]);
					tweet_json.put("user", user_json);

					context.write(null, new Text(tweet_json.toString()));
				}
				// Get a set of customers not talk about the product
				else {
					String tweet = tweet_textGen
							.getNextTweet(-1);
					String date = twitterDateFormat
							.format(timestamp * 1000);

					tweet_json.put("created_at", date);
					tweet_json.put("text", tweet);
					tweet_json.put("id", String.valueOf(tweet_id));

					user_json.put("id", selected_cust[j]);
					tweet_json.put("user", user_json);

					context.write(null, new Text(tweet_json.toString()));
				}
			}

			for (int k = 0; k < tweets_noncustomer_perunit; k++) {
				double flip = randnum.nextDouble();
				// Get a set of non-customers talk about the product
				if (flip <= noncust_mem_prod) {
					int product_id;

					double flip2 = randnum.nextDouble();
					/************************************************************
					 * mention the promoted products
					 ***********************************************************/
					if (flip2 <= promoted_prod_men_prob_noncust) {
						product_id = candidate_promoted_prod.get(randnum
								.nextInt(candidate_promoted_prod.size()));
					} else {
						product_id = randnum.nextInt(num_products);
					}

					String tweet = tweet_textGen
							.getNextTweet(product_id);
					String date = twitterDateFormat
							.format(timestamp * 1000);

					tweet_json.put("created_at", date);
					tweet_json.put("text", tweet);
					tweet_json.put("id_str", String.valueOf(tweet_id));

					user_json.put("id", selected_noncust[k]);
					tweet_json.put("user", user_json);

					context.write(null, new Text(tweet_json.toString()));
				}
				// Get a set of non-customers not talk about the product
				else {
					String tweet = tweet_textGen
							.getNextTweet(-1);
					String date = twitterDateFormat
							.format(timestamp * 1000);

					tweet_json.put("created_at", date);
					tweet_json.put("text", tweet);
					tweet_json.put("id", String.valueOf(tweet_id));

					user_json.put("id", selected_noncust[k]);
					tweet_json.put("user", user_json);

					context.write(null, new Text(tweet_json.toString()));
				}
			}

		}

	}

}

