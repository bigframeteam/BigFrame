package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;

/**
 * A class to aggregate the number of tweets mentioned a specific product.
 * 
 * @author andy
 *
 */
public class TweetByProductHadoop extends HadoopJob {

	public TweetByProductHadoop(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}

	static class TweetByProductMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		/**
		 * The map output is with this format:
		 * 
		 * itemSK|num of tweets|num of users
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			
			String[] fields = value.toString().split("\\|");
					
			String count = fields[2];
					
			context.write(new LongWritable(Long.parseLong(fields[1])), new Text(count + "|" + "1") );
		}
	}
	
	static class TweetByProductReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		
		/**
		 * The reduce output is with this format:
		 * 
		 * itemSK|num of tweets|num of users
		 */
		@Override
	    protected void reduce(LongWritable key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
	    
	    	int sum_tweets = 0;
	    	int sum_users = 0;
	    	for(Text value : values) {
	    		String [] fields = value.toString().split("\\|");
	    		sum_tweets += Integer.parseInt(fields[0]);
	    		sum_users += Integer.parseInt(fields[1]);
	    	}
	    	
	    	context.write(key, new Text(sum_tweets + "|" + sum_users));
	    }
	}
	
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			String hdfs_dir = TwitterRankConstant.TWEET_BY_PRODUCT_PATH();
			Path outputDir = new Path(hdfs_dir);	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(SenAnalyzeConstant.FILTERED_TWEETS_PATH()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(TweetByProductHadoop.class);
			job.setJobName("tweet by product");
			job.setMapperClass(TweetByProductMapper.class);

			job.setCombinerClass(TweetByProductReducer.class);
			job.setReducerClass(TweetByProductReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);

			
			return job.waitForCompletion(true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

}
