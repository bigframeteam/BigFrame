package bigframe.datagen.nested;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.KroneckerGraphGen;
import bigframe.datagen.relational.CollectTPCDSstatNaive;
import bigframe.datagen.relational.TpcdsItemInfo;
import bigframe.datagen.relational.TpcdsPromotionInfo;
import bigframe.datagen.text.TextGenFactory;
import bigframe.datagen.text.TweetTextGen;
import bigframe.datagen.util.RandomSeeds;

/**
 * Single machine raw tweet generator.
 * 
 * @author andy
 * 
 */
public class RawTweetGenNaive extends RawTweetGen {



	public RawTweetGenNaive(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
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

	@Override
	public void generate() {

		System.out.println("Generating raw tweets data");


		TweetTextGen tweet_textGen = TextGenFactory.getTextGenByName(textgen_name);


		if (tweet_textGen == null) {
			System.out.println("Please set the tweet text generator first!");
			System.exit(-1);
		}

		JSONObject tweet_json = RawTweetGenConstants.TWEET_JSON;
		CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();
		tpcds_stat_collecter.genTBLonHDFS(conf, (int) targetGB, RawTweetGenConstants.PROMOTION_TBL);
		tpcds_stat_collecter.genTBLonHDFS(conf, (int) targetGB, RawTweetGenConstants.ITEM_TBL);

		Date dateBegin = stringToDate(RawTweetGenConstants.TWEET_BEGINDATE);
		Date dateEnd = stringToDate(RawTweetGenConstants.TWEET_ENDDATE);

		long time_begin = dateBegin.getTime()/1000;
		long time_end = dateEnd.getTime()/1000;

		
		long total_tweets = getTotalNumTweets();
		
		// Separate twitter account into customer and non customer
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

		tweet_textGen.setRandomSeed(RandomSeeds.SEEDS_TABLE[0]);

		Configuration mapreduce_config = new Configuration();
		mapreduce_config.addResource(new Path(conf.getProp().get(
				BigConfConstants.BIGFRAME_HADOOP_HOME)
				+ "/conf/core-site.xml"));
		TpcdsPromotionInfo promt_info = new TpcdsPromotionInfo();
		tpcds_stat_collecter.collectHDFSPromtTBL(mapreduce_config,
				RawTweetGenConstants.PROMOTION_TBL+".dat", promt_info);
		
		TpcdsItemInfo item_info = new TpcdsItemInfo();
		tpcds_stat_collecter.collectHDFSItemTBL(mapreduce_config, 
				RawTweetGenConstants.ITEM_TBL+".dat", item_info);

		TweetGenDist tweet_gen_dist = new SimpleTweetGenDist(RandomSeeds.SEEDS_TABLE[0], tweet_textGen, 1);
		// The conversion from int to long for time_begin and time_end will lost precision. 
		tweet_gen_dist.init(customer_twitterAcc, non_customer_acc, time_begin, 
				time_end, promt_info, item_info, num_products, tweet_json);
		
		
		List<String> tweet_list = new LinkedList<String>();
		int chunk = 0;
		for(int i = 0; i < total_tweets; i++) {
			tweet_list.add(tweet_gen_dist.getNextTweet().toString());
			if(tweet_list.size()>100000) {
				String filename = "tweets.dat." + String.valueOf(chunk);
				writeToHDFS(hdfs_dir, filename, tweet_list);

				tweet_list = new LinkedList<String>();
				chunk++;
			}
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
