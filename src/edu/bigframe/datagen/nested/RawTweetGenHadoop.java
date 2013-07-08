package edu.bigframe.datagen.nested;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

//import javax.xml.soap.Text;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;

import edu.bigframe.BigFrameDriver;
import edu.bigframe.datagen.DatagenConf;
import edu.bigframe.datagen.graph.KroneckerGraphGen;
import edu.bigframe.datagen.relational.CollectTPCDSstat;
import edu.bigframe.datagen.relational.CollectTPCDSstatNaive;
import edu.bigframe.datagen.relational.PromotionInfo;
import edu.bigframe.datagen.text.TweetTextGenSimple;
import edu.bigframe.util.Constants;
import edu.bigframe.util.RandomSeeds;

public class RawTweetGenHadoop extends RawTweetGen {

	
	public RawTweetGenHadoop(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}


	
	@Override
	public void generate() {
		// TODO Auto-generated method stub
		System.out.println("Generating raw tweets data");
		
		if (textgen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();
		tpcds_stat_collecter.genPromtTBLonHDFS(conf, (int)targetGB);

		
		Date dateBegin = stringToDate(Constants.TWEET_BEGINDATE);
		Date dateEnd = stringToDate(Constants.TWEET_ENDDATE);
		
		int days_between = daysBetween(dateBegin, dateEnd);
		
		// Separate twitter account into customer and non customer 
		//
		//
		//String customer_acc_path = conf.getDataStoredPath().get(Constants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "/" + "twitter_mapping";
		//tpcds_stat_collecter.IntialCustTwitterAcc(customer_acc_path, conf);
		//long [] customer_acc = tpcds_stat_collecter.getCustTwitterAcc();
		
		
		// Calculate the number twitter account based on the graph volume in GB 
		int nested_proportion = conf.getDataScaleProportions().get(Constants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
		int twitter_graph_proportion = conf.getDataScaleProportions().get(Constants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
		int tpcds_proportion = conf.getDataScaleProportions().get(Constants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);
		
		float graph_targetGB = (float) (twitter_graph_proportion * 1.0 /nested_proportion * targetGB);
		float tpcds_targetGB = (float) (tpcds_proportion * 1.0 /nested_proportion * targetGB);
		
		Integer num_products = (int) tpcds_stat_collecter.getNumOfItem((int)tpcds_targetGB);	
		assert(num_products != null);
		int num_twitter_user = (int) KroneckerGraphGen.getNodeCount(graph_targetGB);
		

		long dateBegin_time_sec = dateBegin.getTime()/1000;
		long dateEnd_time_sec = dateEnd.getTime()/1000;
		
		Configuration mapreduce_config = new Configuration();
		mapreduce_config.addResource(new Path(conf.getProp().get(Constants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
		mapreduce_config.addResource(new Path(conf.getProp().get(Constants.BIGFRAME_HADOOP_HOME)+"/conf/mapred-site.xml"));
		
		long tweets_per_day = getTweetsPerDay(days_between);
		long GBPerMapper = RawTweetGenConstants.GB_PER_MAPPER;
		int num_Mapper = (int) Math.ceil(targetGB/GBPerMapper);
		
		
		mapreduce_config.setLong(RawTweetGenConstants.TIME_BEGIN, dateBegin_time_sec);
		mapreduce_config.setLong(RawTweetGenConstants.TIME_END, dateEnd_time_sec);
		mapreduce_config.setInt(RawTweetGenConstants.NUM_MAPPERS, num_Mapper);
		mapreduce_config.setLong(RawTweetGenConstants.TWEETS_PER_DAY, tweets_per_day);
		mapreduce_config.setLong(RawTweetGenConstants.NUM_PRODUCT, num_products);
		mapreduce_config.setLong(RawTweetGenConstants.NUM_TWITTER_USER, num_twitter_user);
		mapreduce_config.setFloat(RawTweetGenConstants.TPCDS_TARGET_GB, tpcds_targetGB);
		mapreduce_config.setFloat(RawTweetGenConstants.GRAPH_TARGET_GB, graph_targetGB);
		
		try {
			Job job = new Job(mapreduce_config);
			
			Path outputDir = new Path(hdfs_dir);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setJarByClass(RawTweetGenMapper.class);
			job.setMapperClass(RawTweetGenMapper.class);
		    job.setNumReduceTasks(0);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    job.setInputFormatClass(RangeInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		  
		    
		    job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	static class RawTweetGenMapper extends Mapper<NullWritable, TweetGenInfoWritable, NullWritable, Text> {
		private static final Logger LOG = Logger.getLogger(RawTweetGenMapper.class); 
		@SuppressWarnings("unchecked")
		@Override
		protected void map(NullWritable ignored, TweetGenInfoWritable tweet_gen_info, final Context context) 
				throws IOException, InterruptedException {
			LOG.info("Begin time: "+tweet_gen_info.begin+";"
				+"End time: " + tweet_gen_info.end + ";" 
				+ "Tweets per day: " + tweet_gen_info.tweets_per_day);
			long time_begin = tweet_gen_info.begin;
			long time_end = tweet_gen_info.end;
			long tweets_per_day = tweet_gen_info.tweets_per_day;
			
			Configuration mapreduce_config = context.getConfiguration();

			int num_twitter_user = mapreduce_config.getInt(RawTweetGenConstants.NUM_TWITTER_USER, 0);
			float tpcds_targetGB = mapreduce_config.getFloat(RawTweetGenConstants.TPCDS_TARGET_GB, 0);
			float graph_targetGB = mapreduce_config.getFloat(RawTweetGenConstants.GRAPH_TARGET_GB, 0);
			int num_products = mapreduce_config.getInt(RawTweetGenConstants.NUM_PRODUCT, 0);
			
			CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
			PromotionInfo promt_info = new PromotionInfo();
			tpcds_stat_collecter.collectHDFSPromtResult(mapreduce_config, "promotion.dat", promt_info);
			
			/*
			 * Get the set of promoted products and their promotion period.
			 */
			ArrayList<Integer> dateBeginSK = promt_info.getDateBeginSK();
			ArrayList<Integer> dateEndSK = promt_info.getDateEndSK();
			ArrayList<Integer> productSK = promt_info.getProductSK();
			
			
			ArrayList<Integer> candidate_promoted_prod = new ArrayList<Integer>();
			
			for(int i = 0; i < dateBeginSK.size(); i++) {
				if(promt_info.getDateBySK(dateBeginSK.get(i)).getTime()/1000 > time_end) {
					continue;
				}

				if(promt_info.getDateBySK(dateEndSK.get(i)).getTime()/1000 < time_begin) {
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
			double promoted_product_men_prob = mention_prob.getPromotedProdMenProbCust();
			double promoted_prod_men_prob_noncust = mention_prob.getPromotedProdMenProbNonCust();

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
			double tweets_per_sec = tweets_per_day * 1.0 / 24 / 3600;
			
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
			
			JSONObject tweet_json = RawTweetGenConstants.TWEET_JSON;
			JSONObject user_json = (JSONObject) tweet_json.get("user"); 
			
			SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			twitterDateFormat.setLenient(false);
			
			RandomEngine twister = new MersenneTwister((int) (RandomSeeds.SEEDS_TABLE[0]+time_begin));
			Random randnum = new Random(RandomSeeds.SEEDS_TABLE[0]+time_begin);
			
			
			long tweet_id = 0;
			// For each time unit, randomly get a set of people to sent tweets 
			for(long timestamp = time_begin; timestamp <= time_end; timestamp+=timeunit) {
				long [] selected_cust = new long[tweets_customer_perunit];
				long [] selected_noncust = new long[tweets_noncustomer_perunit];
				RandomSampler.sample(tweets_customer_perunit, num_cust_acc, tweets_customer_perunit, 1, selected_cust , 0, twister);
				RandomSampler.sample(tweets_noncustomer_perunit, num_noncust_acc, tweets_noncustomer_perunit, 1, selected_noncust , 0, twister);
				
				for(int j = 0; j < tweets_customer_perunit; j++) {
					double flip = randnum.nextDouble();
					// Get a set of customers talk about the product
					if (flip <= cust_mem_prod) {
						
						
						int product_id;
						
						double flip2 = randnum.nextDouble();
						/************************************************************
						 * mention the promoted products
						 ***********************************************************/
						if(flip2 <= promoted_product_men_prob) {
							product_id = candidate_promoted_prod.get(randnum.nextInt(candidate_promoted_prod.size()));
						}
						
						else
							product_id = randnum.nextInt(num_products);
						
						String tweet = RawTweetGenConstants.TEXT_GEN.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);
						
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
						String tweet = RawTweetGenConstants.TEXT_GEN.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);
						
						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id", String.valueOf(tweet_id));
						
						
						user_json.put("id", selected_cust[j]);
						tweet_json.put("user", user_json);
						
						context.write(null, new Text(tweet_json.toString()));
					}
				}
				
				for(int k = 0; k < tweets_noncustomer_perunit; k++) {
					double flip = randnum.nextDouble();
					// Get a set of non-customers talk about the product
					if (flip <= noncust_mem_prod) {
						int product_id;
						
						double flip2 = randnum.nextDouble();
						/************************************************************
						 * mention the promoted products
						 ***********************************************************/
						if(flip2 <= promoted_prod_men_prob_noncust) {
							product_id = candidate_promoted_prod.get(randnum.nextInt(candidate_promoted_prod.size()));
						}
						
						else
							product_id = randnum.nextInt(num_products);
						
						String tweet = RawTweetGenConstants.TEXT_GEN.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);
						
						tweet_json.put("created_at", date);
						tweet_json.put("text", tweet);
						tweet_json.put("id_str", String.valueOf(tweet_id));				
						
						user_json.put("id", selected_noncust[k]);
						tweet_json.put("user", user_json);
						
						context.write(null, new Text(tweet_json.toString()));
					}
					// Get a set of non-customers not talk about the product
					else {
						String tweet = RawTweetGenConstants.TEXT_GEN.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);
						
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
	
	static class TweetGenInfoWritable implements Writable {
	     public Long begin;
	     public Long end;
	     public Long tweets_per_day;

	     public TweetGenInfoWritable() {
	     }

	     public TweetGenInfoWritable(Long begin, Long end, Long tweets_per_day) {
	    	 this.begin = begin;
	    	 this.end = end;
	    	 this.tweets_per_day = tweets_per_day;
	     }

	     @Override
	     public void readFields(DataInput in) throws IOException {
	        if (null == in) {
	           throw new IllegalArgumentException("in cannot be null");
	        }
	        
	        begin = in.readLong();
	        end = in.readLong();
	        tweets_per_day = in.readLong();

	     }

	     @Override
	     public void write(DataOutput out) throws IOException {
	         if (null == out) {
	             throw new IllegalArgumentException("out cannot be null");
	         }
	         out.writeLong(begin);
	         out.writeLong(end);
	         out.writeLong(tweets_per_day);
	     }

	     @Override
	     public String toString() {
	         return "Time begin: " + begin + "\n" 
	        		 + "Time end: " + end + "\n" 
	        		 + "Tweets per day: " + tweets_per_day;
	     }
	}
	
	static class RangeInputFormat  extends InputFormat<NullWritable, TweetGenInfoWritable> {
	    /**
	     * An input split consisting of a range of time.
	     */
		private static final Logger LOG = Logger.getLogger(RangeInputFormat.class);
		static class RangeInputSplit extends InputSplit implements Writable {
			long begin;
			long end;
			long tweets_per_day;

			public RangeInputSplit() { }

			public RangeInputSplit(long begin, long end, long tweets_per_day) {
				this.begin = begin;
				this.end = end;
				this.tweets_per_day = tweets_per_day;
			}

			public long getLength() throws IOException {
				return 0;
			}

			public String[] getLocations() throws IOException {
				return new String[]{};
			}

			@Override
			public void readFields(DataInput in) throws IOException {
				// TODO Auto-generated method stub
				begin = WritableUtils.readVLong(in);
				end = WritableUtils.readVLong(in);
				tweets_per_day = WritableUtils.readVLong(in);

			}

			@Override
			public void write(DataOutput out) throws IOException {
				// TODO Auto-generated method stub
		        WritableUtils.writeVLong(out, begin);
		        WritableUtils.writeVLong(out, end);
		        WritableUtils.writeVLong(out, tweets_per_day);
			}
		}
	
		static class RangeRecordReader extends RecordReader<NullWritable, TweetGenInfoWritable> {
			 
			long begin;
			long end;
			long tweets_per_day;
			NullWritable key = null;
			TweetGenInfoWritable value = null;
			
			public RangeRecordReader() {
			}
			  
			public void initialize(InputSplit split, TaskAttemptContext context) 
			      throws IOException, InterruptedException {
				begin = ((RangeInputSplit)split).begin;
				end = ((RangeInputSplit)split).end;;
				tweets_per_day = ((RangeInputSplit)split).tweets_per_day;
			}
			
			public void close() throws IOException {
			    // NOTHING
			}
			
			public NullWritable getCurrentKey() {
				return key;
			}
			
			public TweetGenInfoWritable getCurrentValue() {
			    return value;
			}
			
			public float getProgress() throws IOException {
			    return 0;
			}
			
			public boolean nextKeyValue() {
				if (key == null && value == null) {
					value = new TweetGenInfoWritable(begin, end, tweets_per_day);
					return true;
				}
				else
					return false;

			}
			      
		}

		@Override
		public RecordReader<NullWritable, TweetGenInfoWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new RangeRecordReader();
		}
	

	    /**
	     * Create the desired number of splits, dividing the number of seconds
	     * between the mappers.
	     */
		public List<InputSplit> getSplits(JobContext job) {
		    long time_begin = getTimeBegin(job);
		    long time_end = getTimeEnd(job);
		    long total_time = time_end - time_begin;
		    int numSplits = job.getConfiguration().getInt(RawTweetGenConstants.NUM_MAPPERS, 1);
		    int tweets_per_day = job.getConfiguration().getInt(RawTweetGenConstants.TWEETS_PER_DAY, 1);
		    LOG.info("Generating total seconds " + total_time + " using " + numSplits);
		    List<InputSplit> splits = new ArrayList<InputSplit>();
		    long begin = time_begin;
		    long duration = (long) Math.ceil(total_time * 1.0 / numSplits);
		    for(int split = 0; split < numSplits; ++split) {
		        splits.add(new RangeInputSplit(begin, duration + begin, tweets_per_day));
		        begin += duration;
		    }
		    return splits;
	    }

		public long getTimeBegin(JobContext job) {
			return job.getConfiguration().getLong(RawTweetGenConstants.TIME_BEGIN, 0);
		}
			  
		public void setTimeBegin(Job job, long time_begin) {
			job.getConfiguration().setLong(RawTweetGenConstants.TIME_BEGIN, time_begin);
		}		
		
		public long getTimeEnd(JobContext job) {
			return job.getConfiguration().getLong(RawTweetGenConstants.TIME_END, 0);
		}
			  
		public void setTimeEnd(Job job, long time_end) {
			job.getConfiguration().setLong(RawTweetGenConstants.TIME_END, time_end);
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
