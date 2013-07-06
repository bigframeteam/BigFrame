package edu.bigframe.datagen.nested;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import javax.xml.soap.Text;


import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
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
import edu.bigframe.datagen.DataGenerator;
import edu.bigframe.datagen.DatagenConf;
import edu.bigframe.datagen.graph.KroneckerGraphGen;
import edu.bigframe.datagen.relational.CollectTPCDSstat;
import edu.bigframe.datagen.relational.CollectTPCDSstatNaive;
import edu.bigframe.datagen.text.TweetTextGenSimple;
import edu.bigframe.util.Constants;
import edu.bigframe.util.RandomSeeds;

public class RawTweetGenHadoop extends RawTweetGen {

	private static final String NUM_MAPPERS = "mapreduce.rawtweet.num-mappers";
	private static final String TWEETS_PER_DAY = "mapreduce.rawtweet.tweet-per-day";
	private static final String TIME_BEGIN = "mapreduce.rawtweet.time-begin";
	private static final String TIME_END = "mapreduce.rawtweet.time-end";
	
	private static final String TPCDS_TARGET_GB = "mapreduce.rawtweet.tpcds-target-GB";
	private static final String GRAPH_TARGET_GB = "mapreduce.rawtweet.graph-target-GB";
	private static final String NESTED_TARGET_GB = "mapreduce.rawtweet.nested-targetGB";
	
	private static final String NUM_PRODUCT = "mapreduce.rawtweet.num-product";
	private static final String NUM_TWITTER_USER = "mapreduce.rawtweet.num-twitter-user";
	
	private static final String TWEET_TEMPLATE = "tweet.json";
	private static final String SENTIMENT_DICT = "sentiment.dict";
	
	private static final TweetTextGenSimple TEXT_GEN = new TweetTextGenSimple(null, 0);
	private static final InputStream TWEET_TEMPLATE_FILE = BigFrameDriver.class.getClassLoader().getResourceAsStream("tweet_template.json");
	private static final JSONObject TWEET_JSON = parseJsonFromFile(TWEET_TEMPLATE_FILE);
	
	public RawTweetGenHadoop(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

/*	
	private void cacheFile(String filename, InputStream input, Configuration mapreduce_config) {
		try {
			//Path path = new Path(dir);
			mapreduce_config.addResource(new Path(conf.getProp().get(Constants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
			FileSystem fileSystem = FileSystem.get(mapreduce_config);

						
			Path path = new Path(filename);
			if (fileSystem.exists(path))
				fileSystem.delete(path, true);
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path, true)));
			//BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(path+"/"+filename), true)));
			
			BufferedReader br = new BufferedReader(new InputStreamReader(input));
			
			String line;
			while ((line = br.readLine()) != null) {
				bufferedWriter.write(line);
				//bufferedWriter.newLine();
			}
			bufferedWriter.close();
			
			DistributedCache.addCacheFile(path.toUri(), mapreduce_config);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}*/
	
	static JSONObject parseJsonFromFile(InputStream file) {
		JSONParser parser = new JSONParser();
		try {
			 
			Object obj = parser.parse(new BufferedReader(new InputStreamReader(file)));
	 
			JSONObject jsonObject = (JSONObject) obj;
	 
			return jsonObject;
	
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	@Override
	public void generate() {
		// TODO Auto-generated method stub
		System.out.println("Generating raw tweets data");
		
		if (textgen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();

		
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
		
		//cacheFile(TWEET_TEMPLATE, tweet_template_file, mapreduce_config);
		//cacheFile(SENTIMENT_DICT, dictionary_file, mapreduce_config);
		
		long tweets_per_day = getTweetsPerDay(days_between);
		long GBPerMapper = 2;
		int num_Mapper = (int) Math.ceil(targetGB/GBPerMapper);
		
		
		mapreduce_config.setLong(TIME_BEGIN, dateBegin_time_sec);
		mapreduce_config.setLong(TIME_END, dateEnd_time_sec);
		mapreduce_config.setInt(NUM_MAPPERS, num_Mapper);
		mapreduce_config.setLong(TWEETS_PER_DAY, tweets_per_day);
		mapreduce_config.setLong(NUM_PRODUCT, num_products);
		mapreduce_config.setLong(NUM_TWITTER_USER, num_twitter_user);
		mapreduce_config.setFloat(TPCDS_TARGET_GB, tpcds_targetGB);
		mapreduce_config.setFloat(GRAPH_TARGET_GB, graph_targetGB);
		
		try {
			Job job = new Job(mapreduce_config);
			
			Path outputDir = new Path(hdfs_dir);
			FileOutputFormat.setOutputPath(job, outputDir);
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
/*			LOG.info("Begin time: "+tweet_gen_info.begin+";"
				+"End time: " + tweet_gen_info.end + ";" 
				+ "Tweets per day: " + tweet_gen_info.tweets_per_day);*/
			long time_begin = tweet_gen_info.begin;
			long time_end = tweet_gen_info.end;
			long tweets_per_day = tweet_gen_info.tweets_per_day;
			
			Configuration mapreduce_config = context.getConfiguration();

			int num_twitter_user = mapreduce_config.getInt(NUM_TWITTER_USER, 0);
			float tpcds_targetGB = mapreduce_config.getFloat(TPCDS_TARGET_GB, 0);
			float graph_targetGB = mapreduce_config.getFloat(GRAPH_TARGET_GB, 0);
			int num_products = mapreduce_config.getInt(NUM_PRODUCT, 0);
			
			
			ProductMentionProb mention_prob = new ProductMentionProb();
			
			double cust_mem_prod = mention_prob.getCustMenProb();
			double noncust_mem_prod = mention_prob.getNonCustMenProb();

			CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
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
			
			JSONObject user_json = (JSONObject) TWEET_JSON.get("user"); 
			
			SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			twitterDateFormat.setLenient(false);
			
			RandomEngine twister = new MersenneTwister(RandomSeeds.SEEDS_TABLE[0]);
			Random randnum = new Random(RandomSeeds.SEEDS_TABLE[0]);
			
			
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
						int product_id = randnum.nextInt(num_products);
						String tweet = TEXT_GEN.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);
						
						TWEET_JSON.put("created_at", date);
						TWEET_JSON.put("text", tweet);
						TWEET_JSON.put("id", String.valueOf(tweet_id));
						
						// How to put nested attribute?
						
						user_json.put("id", selected_cust[j]);
						TWEET_JSON.put("user", user_json);
						
						context.write(null, new Text(TWEET_JSON.toString()));
					}
					// Get a set of customers not talk about the product
					else {
						String tweet = TEXT_GEN.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);
						
						TWEET_JSON.put("created_at", date);
						TWEET_JSON.put("text", tweet);
						TWEET_JSON.put("id", String.valueOf(tweet_id));
						
						
						user_json.put("id", selected_cust[j]);
						TWEET_JSON.put("user", user_json);
						
						context.write(null, new Text(TWEET_JSON.toString()));
					}
				}
				
				for(int k = 0; k < tweets_noncustomer_perunit; k++) {
					double flip = randnum.nextDouble();
					// Get a set of non-customers talk about the product
					if (flip <= noncust_mem_prod) {
						int product_id = randnum.nextInt(num_products);
						
						String tweet = TEXT_GEN.getNextTweet(product_id);
						String date = twitterDateFormat.format(timestamp*1000);
						
						TWEET_JSON.put("created_at", date);
						TWEET_JSON.put("text", tweet);
						TWEET_JSON.put("id_str", String.valueOf(tweet_id));				
						
						user_json.put("id", selected_noncust[k]);
						TWEET_JSON.put("user", user_json);
						
						context.write(null, new Text(TWEET_JSON.toString()));
					}
					// Get a set of non-customers not talk about the product
					else {
						String tweet = TEXT_GEN.getNextTweet(-1);
						String date = twitterDateFormat.format(timestamp*1000);
						
						TWEET_JSON.put("created_at", date);
						TWEET_JSON.put("text", tweet);
						TWEET_JSON.put("id", String.valueOf(tweet_id));
						
						user_json.put("id", selected_noncust[k]);
						TWEET_JSON.put("user", user_json);
						
						context.write(null, new Text(TWEET_JSON.toString()));
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
		    int numSplits = job.getConfiguration().getInt(NUM_MAPPERS, 1);
		    int tweets_per_day = job.getConfiguration().getInt(TWEETS_PER_DAY, 1);
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
			return job.getConfiguration().getLong(TIME_BEGIN, 0);
		}
			  
		public void setTimeBegin(Job job, long time_begin) {
			job.getConfiguration().setLong(TIME_BEGIN, time_begin);
		}		
		
		public long getTimeEnd(JobContext job) {
			return job.getConfiguration().getLong(TIME_END, 0);
		}
			  
		public void setTimeEnd(Job job, long time_end) {
			job.getConfiguration().setLong(TIME_END, time_end);
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
