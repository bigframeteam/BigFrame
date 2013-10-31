package bigframe.refreshing.nested;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.appDomainInfo.BIDomainDataInfo;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.datagen.nested.tweet.RawTweetGenConstants;
import bigframe.datagen.relational.tpcds.CollectTPCDSstatNaive;
import bigframe.datagen.relational.tpcds.TpcdsItemInfo;
import bigframe.datagen.relational.tpcds.TpcdsPromotionInfo;
import bigframe.datagen.text.TextGenFactory;
import bigframe.datagen.text.tweet.TweetTextGen;
import bigframe.datagen.util.RandomSeeds;
import bigframe.refreshing.DataProducer;
import bigframe.util.parser.JsonParser;

public class TweetDataProducer extends DataProducer {

	private StreamTweetGenDist tweet_gen_dist;
	
	private final InputStream TWEET_TEMPLATE_FILE = TweetDataProducer.class.
			getClassLoader().getResourceAsStream("tweet_template.json");
	
	public TweetDataProducer(BigDataInputFormat bigdataIF) {
		super(bigdataIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void init() {
		
		CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();
		tpcds_stat_collecter.genTBLonHDFS(bigdataIF, (int) BIDomainDataInfo.R_STREAM_GB, RawTweetGenConstants.PROMOTION_TBL);
		tpcds_stat_collecter.genTBLonHDFS(bigdataIF, (int) BIDomainDataInfo.R_STREAM_GB, RawTweetGenConstants.ITEM_TBL);
		
		Configuration mapred_config = new Configuration();
		
		mapred_config.addResource(new Path(bigdataIF.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
		mapred_config.addResource(new Path(bigdataIF.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/mapred-site.xml"));	
		
		TpcdsPromotionInfo promt_info = new TpcdsPromotionInfo();
		TpcdsItemInfo item_info = new TpcdsItemInfo();
		
		FileSystem fs;
		try {
			fs = FileSystem.get(mapred_config);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(
					fs.open(new Path(RawTweetGenConstants.PROMOTION_TBL + ".dat"))));
			tpcds_stat_collecter.setPromtResult(in, promt_info);
			
			in = new BufferedReader(new InputStreamReader(
					fs.open(new Path(RawTweetGenConstants.ITEM_TBL + ".dat"))));
			
			tpcds_stat_collecter.setItemResult(in, item_info);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Integer num_products = (int) tpcds_stat_collecter
				.getNumOfItem((int) BIDomainDataInfo.R_STREAM_GB);
		assert (num_products != null);
		int num_twitter_user = (int) KroneckerGraphGen
				.getNodeCount(BIDomainDataInfo.G_STREAM_GB);
		
		long[] customer_twitterAcc = tpcds_stat_collecter
				.getCustTwitterAcc(BIDomainDataInfo.R_STREAM_GB, BIDomainDataInfo.G_STREAM_GB);
		long[] non_customer_acc = tpcds_stat_collecter
				.getNonCustTwitterAcc(customer_twitterAcc, num_twitter_user);

		TweetTextGen tweet_textGen = TextGenFactory
				.getTextGenByName("simple");
		tweet_textGen.setRandomSeed(RandomSeeds.SEEDS_TABLE[0]);
		
		JSONObject tweet_json = JsonParser.parseJsonFromFile(TWEET_TEMPLATE_FILE);
		
		tweet_gen_dist = new StreamTweetGenDist(RandomSeeds.SEEDS_TABLE[0], tweet_textGen, 1);
		tweet_gen_dist.init(customer_twitterAcc, non_customer_acc, 0, 
				0, promt_info, item_info, num_products, tweet_json);
		

	}
	
	class TweetProducer implements Runnable{
		
		ProducerConfig config;
		
		private BigDataInputFormat bigdataIF;
		
		public TweetProducer(BigDataInputFormat bigdataIF, ProducerConfig config) {
			
			this.bigdataIF = bigdataIF;
			this.config = config;
			
		}

		@Override
		public void run() {
			
			Producer<String, String> producer = new Producer<String, String>(config);
			
	        /**
	         * Try to sent 10000 tweets per second
	         */
			while(true) {
				long start_time = System.currentTimeMillis();
				for(int i = 0; i < 10000; i++) {
					tweet_gen_dist.setTimeStamp(System.currentTimeMillis());
					KeyedMessage<String, String> data2 = new KeyedMessage<String, String>("tweets", tweet_gen_dist.getNextTweet());
					producer.send(data2);				
				}
				long stop_time = System.currentTimeMillis();

				try {
					
					if(stop_time-start_time <= 1000)					
						Thread.sleep(1000 - (stop_time-start_time));
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}		
	}

	@Override
	public void produce() {
		ExecutorService pool = Executors.newFixedThreadPool(1);
		
		TweetProducer t_producer = new TweetProducer(bigdataIF, config);
		
		pool.submit(t_producer);

	}

}
