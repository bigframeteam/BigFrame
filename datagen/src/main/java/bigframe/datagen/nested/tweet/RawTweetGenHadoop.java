package bigframe.datagen.nested.tweet;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.datagen.relational.tpcds.CollectTPCDSstatNaive;


/**
 * Hadoop implementation of the raw tweet generator.
 * 
 * @author andy
 * 
 */
public class RawTweetGenHadoop extends RawTweetGen {

	public RawTweetGenHadoop(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

	private void cleanUP(Configuration mapreduce_config) {
		deleteFileOnHDFS(mapreduce_config, RawTweetGenConstants.PROMOTION_TBL + ".dat");
		deleteFileOnHDFS(mapreduce_config, RawTweetGenConstants.ITEM_TBL + ".dat");
	}

	private void deleteFileOnHDFS(Configuration mapreduce_config, String filename) {
		Path hdfs_path = new Path(filename);

		try {
			FileSystem fileSystem = FileSystem.get(mapreduce_config);
			if (fileSystem.exists(hdfs_path)) {
				fileSystem.delete(hdfs_path, true);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void generate() {
		// TODO Auto-generated method stub
		System.out.println("Generating raw tweets data!");

		CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();
		tpcds_stat_collecter.genTBLonHDFS(conf, (int) targetGB, RawTweetGenConstants.PROMOTION_TBL);
		tpcds_stat_collecter.genTBLonHDFS(conf, (int) targetGB, RawTweetGenConstants.ITEM_TBL);

		Date dateBegin = stringToDate(RawTweetGenConstants.TWEET_BEGINDATE);
		Date dateEnd = stringToDate(RawTweetGenConstants.TWEET_ENDDATE);


		// Separate twitter account into customer and non customer
		//
		// Calculate the number twitter account based on the graph volume in GB
		float nested_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
		float twitter_graph_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
		float tpcds_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);

		float graph_targetGB = twitter_graph_proportion
				/ nested_proportion * targetGB;
		float tpcds_targetGB = tpcds_proportion
				/ nested_proportion * targetGB;

		Integer num_products = (int) tpcds_stat_collecter
				.getNumOfItem((int) tpcds_targetGB);
		assert (num_products != null);
		int num_twitter_user = (int) KroneckerGraphGen
				.getNodeCount(graph_targetGB);

		long dateBegin_time_sec = dateBegin.getTime() / 1000;
		long dateEnd_time_sec = dateEnd.getTime() / 1000;

		Configuration mapred_config = new Configuration();
		mapred_config.addResource(new Path(conf.getProp().get(
				BigConfConstants.BIGFRAME_HADOOP_HOME)
				+ "/conf/core-site.xml"));
		mapred_config.addResource(new Path(conf.getProp().get(
				BigConfConstants.BIGFRAME_HADOOP_HOME)
				+ "/conf/mapred-site.xml"));

		try {
			DistributedCache.addCacheFile(new URI(RawTweetGenConstants.PROMOTION_TBL+".dat"), 
					mapred_config);
			DistributedCache.addCacheFile(new URI(RawTweetGenConstants.ITEM_TBL+".dat"), 
					mapred_config);
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//long tweets_per_day = getTweetsPerDay(days_between);
		int GBPerMapper = RawTweetGenConstants.GB_PER_MAPPER;
		long tweets_per_mapper = getNumTweetsBySize(GBPerMapper);
		long total_tweets = getTotalNumTweets();

		int num_Mapper = (int) Math.ceil(total_tweets * 1.0 / tweets_per_mapper);
		
		mapred_config.setLong(RawTweetGenConstants.TIME_BEGIN,
				dateBegin_time_sec);
		mapred_config.setLong(RawTweetGenConstants.TIME_END,
				dateEnd_time_sec);
		mapred_config.setInt(RawTweetGenConstants.NUM_MAPPERS, num_Mapper);
//		mapreduce_config.setLong(RawTweetGenConstants.TWEETS_PER_DAY,
//				tweets_per_day);
		mapred_config.setLong(RawTweetGenConstants.TWEETS_PER_MAPPER,
				tweets_per_mapper);
		mapred_config
		.setLong(RawTweetGenConstants.NUM_PRODUCT, num_products);
		mapred_config.setLong(RawTweetGenConstants.NUM_TWITTER_USER,
				num_twitter_user);
		mapred_config.setFloat(RawTweetGenConstants.TPCDS_TARGET_GB,
				tpcds_targetGB);
		mapred_config.setFloat(RawTweetGenConstants.GRAPH_TARGET_GB,
				graph_targetGB);
		mapred_config.set(RawTweetGenConstants.TWEETGEN_NAME, textgen_name);

		try {
			Job job = new Job(mapred_config);

			Path outputDir = new Path(hdfs_dir);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setJarByClass(RawTweetMapper.class);
			job.setMapperClass(RawTweetMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(RangeInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.waitForCompletion(true);

			cleanUP(mapred_config);
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
