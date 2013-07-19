package bigframe.datagen.nested;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigframe.BigConfConstants;
import bigframe.datagen.DatagenConf;
import bigframe.datagen.graph.KroneckerGraphGen;
import bigframe.datagen.relational.CollectTPCDSstatNaive;


/**
 * Hadoop implementation of the raw tweet generator.
 * 
 * @author andy
 * 
 */
public class RawTweetGenHadoop extends RawTweetGen {

	public RawTweetGenHadoop(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

	private void cleanUP(Configuration mapreduce_config) {
		Path hdfs_path = new Path(RawTweetGenConstants.PROMOTION_TBL);

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
		System.out.println("Generating raw tweets data");

		CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();
		tpcds_stat_collecter.genPromtTBLonHDFS(conf, (int) targetGB);

		Date dateBegin = stringToDate(RawTweetGenConstants.TWEET_BEGINDATE);
		Date dateEnd = stringToDate(RawTweetGenConstants.TWEET_ENDDATE);


		// Separate twitter account into customer and non customer
		//
		// Calculate the number twitter account based on the graph volume in GB
		int nested_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
		int twitter_graph_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
		int tpcds_proportion = conf.getDataScaleProportions().get(
				BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);

		float graph_targetGB = (float) (twitter_graph_proportion * 1.0
				/ nested_proportion * targetGB);
		float tpcds_targetGB = (float) (tpcds_proportion * 1.0
				/ nested_proportion * targetGB);

		Integer num_products = (int) tpcds_stat_collecter
				.getNumOfItem((int) tpcds_targetGB);
		assert (num_products != null);
		int num_twitter_user = (int) KroneckerGraphGen
				.getNodeCount(graph_targetGB);

		long dateBegin_time_sec = dateBegin.getTime() / 1000;
		long dateEnd_time_sec = dateEnd.getTime() / 1000;

		Configuration mapreduce_config = new Configuration();
		mapreduce_config.addResource(new Path(conf.getProp().get(
				BigConfConstants.BIGFRAME_HADOOP_HOME)
				+ "/conf/core-site.xml"));
		mapreduce_config.addResource(new Path(conf.getProp().get(
				BigConfConstants.BIGFRAME_HADOOP_HOME)
				+ "/conf/mapred-site.xml"));

		//long tweets_per_day = getTweetsPerDay(days_between);
		int GBPerMapper = RawTweetGenConstants.GB_PER_MAPPER;
		long tweets_per_mapper = getNumTweetsBySize(GBPerMapper);
		long total_tweets = getTotalNumTweets();

		int num_Mapper = (int) Math.ceil(total_tweets * 1.0 / tweets_per_mapper);
		
		mapreduce_config.setLong(RawTweetGenConstants.TIME_BEGIN,
				dateBegin_time_sec);
		mapreduce_config.setLong(RawTweetGenConstants.TIME_END,
				dateEnd_time_sec);
		mapreduce_config.setInt(RawTweetGenConstants.NUM_MAPPERS, num_Mapper);
//		mapreduce_config.setLong(RawTweetGenConstants.TWEETS_PER_DAY,
//				tweets_per_day);
		mapreduce_config.setLong(RawTweetGenConstants.TWEETS_PER_MAPPER,
				tweets_per_mapper);
		mapreduce_config
		.setLong(RawTweetGenConstants.NUM_PRODUCT, num_products);
		mapreduce_config.setLong(RawTweetGenConstants.NUM_TWITTER_USER,
				num_twitter_user);
		mapreduce_config.setFloat(RawTweetGenConstants.TPCDS_TARGET_GB,
				tpcds_targetGB);
		mapreduce_config.setFloat(RawTweetGenConstants.GRAPH_TARGET_GB,
				graph_targetGB);
		mapreduce_config.set(RawTweetGenConstants.TWEETGEN_NAME, textgen_name);

		try {
			Job job = new Job(mapreduce_config);

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

			cleanUP(mapreduce_config);
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
