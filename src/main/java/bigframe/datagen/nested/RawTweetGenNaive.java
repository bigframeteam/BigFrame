package bigframe.datagen.nested;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import bigframe.BigConfConstants;
import bigframe.datagen.DatagenConf;
import bigframe.datagen.graph.KroneckerGraphGen;
import bigframe.datagen.relational.CollectTPCDSstat;
import bigframe.datagen.relational.CollectTPCDSstatNaive;
import bigframe.datagen.text.TextGenFactory;
import bigframe.datagen.text.TweetTextGen;
import bigframe.util.RandomSeeds;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;

/**
 * Single machine raw tweet generator.
 * 
 * @author andy
 * 
 */
public class RawTweetGenNaive extends RawTweetGen {



	public RawTweetGenNaive(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}


	private JSONObject parseJsonFromFile(InputStream file) throws org.json.simple.parser.ParseException {
		JSONParser parser = new JSONParser();
		try {

			Object obj = parser.parse(new BufferedReader(new InputStreamReader(file)));

			JSONObject jsonObject = (JSONObject) obj;

			return jsonObject;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private void writeToHDFS(String dir, String filename, List<String> tweet_list) {
		try {
			Path path = new Path(dir);
			Configuration config = new Configuration();
			config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
			FileSystem fileSystem = FileSystem.get(config);
			if (!fileSystem.exists(path))
				fileSystem.mkdirs(path);

			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(path+"/"+filename), true)));

			for (String tweet : tweet_list) {
				bufferedWriter.write(tweet);
				bufferedWriter.newLine();
			}
			bufferedWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void generate() {

		System.out.println("Generating raw tweets data");


		TweetTextGen textgen = TextGenFactory.getTextGenByName(textgen_name);


		if (textgen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		JSONObject tweet_json;
		CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
		ProductMentionProb mention_prob = new ProductMentionProb();

		double cust_mem_prod = mention_prob.getCustMenProb();
		double noncust_mem_prod = mention_prob.getNonCustMenProb();



		Date dateBegin = stringToDate(RawTweetGenConstants.TWEET_BEGINDATE);
		Date dateEnd = stringToDate(RawTweetGenConstants.TWEET_ENDDATE);

		int days_between = daysBetween(dateBegin, dateEnd);

		// Separate twitter account into customer and non customer
		//
		//
		//String customer_acc_path = conf.getDataStoredPath().get(Constants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "/" + "twitter_mapping";
		//tpcds_stat_collecter.IntialCustTwitterAcc(customer_acc_path, conf);
		//long [] customer_acc = tpcds_stat_collecter.getCustTwitterAcc();


		// Calculate the number twitter account based on the graph volume in GB
		int nested_proportion = conf.getDataScaleProportions().get(BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
		int twitter_graph_proportion = conf.getDataScaleProportions().get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
		int tpcds_proportion = conf.getDataScaleProportions().get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);

		float graph_targetGB = (float) (twitter_graph_proportion * 1.0 /nested_proportion * targetGB);
		float tpcds_targetGB = (float) (tpcds_proportion * 1.0 /nested_proportion * targetGB);

		Integer num_products = (int) tpcds_stat_collecter.getNumOfItem((int)tpcds_targetGB);

		assert(num_products != null);

		int num_twitter_user = (int) KroneckerGraphGen.getNodeCount(graph_targetGB);
		long [] customer_twitterAcc = tpcds_stat_collecter.getCustTwitterAcc(tpcds_targetGB, graph_targetGB);
		long [] non_customer_acc = tpcds_stat_collecter.getNonCustTwitterAcc(customer_twitterAcc, num_twitter_user);

		int num_cust_acc = customer_twitterAcc.length;
		int num_noncust_acc = non_customer_acc.length;

		assert (num_cust_acc != 0);

		/**
		 *  Calculate tweets sent per time unit
		 */

		// Initialise the time unit to be 1 second.
		int timeunit = 1;
		//double tweets_per_sec = num_twitter_user * conf.getDataVelocity().get(Constants.BIGFRAME_DATAVELOCITY_NESTED)
		//* 1.0 / 3600 / 24;

		double tweets_per_sec = getTweetsPerDay(days_between) * 1.0 / 24 / 3600;

		long tweets_per_timeunit = (long) tweets_per_sec;

		//Compute the time unit such that there will be at least one tweet sent
		//in this time unit
		int exponent = 1;
		while (tweets_per_timeunit == 0) {
			tweets_per_timeunit = (long) (tweets_per_sec*Math.pow(10, exponent));
			timeunit = timeunit * 10;
			exponent++;
		}


		// Number of tweets per time unit sent by customer and non-customer respectively
		int tweets_customer_perunit =  (int) (tweets_per_timeunit * num_cust_acc * 1.0/num_twitter_user);
		int tweets_noncustomer_perunit =  (int) (tweets_per_timeunit * num_noncust_acc * 1.0/num_twitter_user);

		//Compute the time unit such that there will be at least one tweet sent by customer
		//in this time unit
		exponent = 1;
		while (tweets_customer_perunit == 0) {
			tweets_customer_perunit = (int) (tweets_per_timeunit * num_cust_acc * 1.0/num_twitter_user * Math.pow(10, exponent));
			tweets_noncustomer_perunit =  (int) (tweets_per_timeunit * num_noncust_acc * 1.0/num_twitter_user * Math.pow(10, exponent));
			timeunit = timeunit * 10;
			exponent++;
		}



		long dateBegin_time_sec = dateBegin.getTime()/1000;
		long dateEnd_time_sec = dateEnd.getTime()/1000;

		try {
			tweet_json = parseJsonFromFile(tweet_template_file);
			JSONObject user_json = (JSONObject) tweet_json.get("user");

			SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			twitterDateFormat.setLenient(false);

			RandomEngine twister = new MersenneTwister(RandomSeeds.SEEDS_TABLE[0]);
			Random randnum = new Random(RandomSeeds.SEEDS_TABLE[0]);

			List<String> tweet_list = new LinkedList<String>();
			int chunk = 0;

			long tweet_id = 0;
			// For each time unit, randomly get a set of people to sent tweets
			for(long timestamp = dateBegin_time_sec; timestamp <= dateEnd_time_sec; timestamp+=timeunit) {
				long [] selected_cust = new long[tweets_customer_perunit];
				long [] selected_noncust = new long[tweets_noncustomer_perunit];
				RandomSampler.sample(tweets_customer_perunit, num_cust_acc, tweets_customer_perunit, 1, selected_cust , 0, twister);
				RandomSampler.sample(tweets_noncustomer_perunit, num_noncust_acc, tweets_noncustomer_perunit, 1, selected_noncust , 0, twister);

				for(int j = 0; j < tweets_customer_perunit; j++) {
					double flip = randnum.nextDouble();
					// Get a set of customers talk about the product
					if (flip <= cust_mem_prod) {
						int product_id = randnum.nextInt(num_products);
						String tweet = textgen.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);

						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id", String.valueOf(tweet_id));

						// How to put nested attribute?

						user_json.put("id", selected_cust[j]);
						tweet_json.put("user", user_json);

						tweet_list.add(tweet_json.toString());
					}
					// Get a set of customers not talk about the product
					else {
						String tweet = textgen.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);

						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id", String.valueOf(tweet_id));


						user_json.put("id", selected_cust[j]);
						tweet_json.put("user", user_json);

						tweet_list.add(tweet_json.toString());
					}
				}

				for(int k = 0; k < tweets_noncustomer_perunit; k++) {
					double flip = randnum.nextDouble();
					// Get a set of non-customers talk about the product
					if (flip <= noncust_mem_prod) {
						int product_id = randnum.nextInt(num_products);

						String tweet = textgen.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);

						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id_str", String.valueOf(tweet_id));

						user_json.put("id", selected_noncust[k]);
						tweet_json.put("user", user_json);

						tweet_list.add(tweet_json.toString());
					}
					// Get a set of non-customers not talk about the product
					else {
						String tweet = textgen.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);

						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id", String.valueOf(tweet_id));

						user_json.put("id", selected_noncust[k]);
						tweet_json.put("user", user_json);
						tweet_list.add(tweet_json.toString());
					}
				}

				if(tweet_list.size()>200000) {
					String filename = "tweets.dat." + String.valueOf(chunk);
					writeToHDFS(hdfs_dir, filename, tweet_list);

					tweet_list = new LinkedList<String>();
					chunk++;
				}

			}


		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}

	@Override
	public int getAbsSizeBySF(int sf) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getSFbyAbsSize(int absSize) {
		// TODO Auto-generated method stub
		return 0;
	}

}
