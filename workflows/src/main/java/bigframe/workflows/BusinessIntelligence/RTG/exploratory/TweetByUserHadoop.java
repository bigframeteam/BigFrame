package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * A class to aggregate the number of tweets sent by a user.
 * 
 * @author andy
 *
 */
public class TweetByUserHadoop extends HadoopJob {

	public TweetByUserHadoop(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}


	static class TweetByUserMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		/**
		 * The map input is with this format:
		 * 
		 * userID|itemSK|num of tweets|the aggregated tweet text
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			
			String[] fields = value.toString().split("\\|");
					
			IntWritable count = new IntWritable(Integer.parseInt(fields[2]));
					
			context.write(new LongWritable(Long.parseLong(fields[0])), count);
		}
	}
	
	static class TweetByUserReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		
		/**
		 * The reduce output is with this format:
		 * 
		 * userID|num of tweets
		 */
		@Override
	    protected void reduce(LongWritable key, Iterable<IntWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
	    
	    	int sum_tweets = 0;
	    	for(IntWritable value : values) {
	    		sum_tweets += value.get();
	    	}
	    	
	    	context.write(key, new IntWritable(sum_tweets));
	    }
	}
	
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			String hdfs_dir = TwitterRankConstant.TWEET_BY_USER_PATH();
			Path outputDir = new Path(hdfs_dir);	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(SenAnalyzeConstant.FILTERED_TWEETS_PATH()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(TweetByUserHadoop.class);
			job.setJobName("tweet by user");
			job.setMapperClass(TweetByUserMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setCombinerClass(TweetByUserReducer.class);
			job.setReducerClass(TweetByUserReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
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
