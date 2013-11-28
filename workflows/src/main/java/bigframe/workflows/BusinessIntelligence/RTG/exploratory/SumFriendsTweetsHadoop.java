package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
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

/**
 * A class to compute the number of tweets sent by a user's friends.
 * It is used by the TwitterRank algorithm.
 * 
 * @author andy
 *
 */
public class SumFriendsTweetsHadoop extends HadoopJob {

	private String graph_path;
	
	public SumFriendsTweetsHadoop(String graph_path, Configuration mapred_config) {
		super(mapred_config);
		this.graph_path = graph_path;
	}

	static class SumFriendsTweetsMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		
		private Map<Long, Integer> map_user_tweets = new HashMap<Long, Integer>();
		
		@Override
		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();
			Path [] uris = DistributedCache.getLocalCacheFiles(mapred_config);
  	   
	  	   	for(Path p : uris) {  		   
	  	   		if (p.toString().contains("part")) {
	  	   			BufferedReader in = new BufferedReader(new FileReader(p.toString()));
	  	   			String line = in.readLine();
	  	   			while (line != null) {
	  	   				String [] fields = line.split("\\|");

	    				String twitterID = fields[0];
	    				String num_tweets =	fields[1];

	    				if (!twitterID.equals("") && !num_tweets.equals("")) {
	    					map_user_tweets.put(Long.parseLong(twitterID), Integer.parseInt(num_tweets));
	    				}
	    				line = in.readLine();
	  	   			}
	  	   			in.close();
	  	   		}
	  	   		
	  	   	}
		}
		
		/**
		 * The output of map function is with this format:
		 * 
		 * follower|num of frend's tweets
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						
			String [] fields = value.toString().split("\\|");
			long friend = 0;
			long follower = 0;
			
			if(!fields[0].equals("") && !fields[1].equals("")) {
				friend = Long.parseLong(fields[0]);
				follower = Long.parseLong(fields[1]);
			}
			
			if(map_user_tweets.containsKey(follower)) {
				if(map_user_tweets.containsKey(friend)) {
					context.write(new LongWritable(follower), 
							new IntWritable(map_user_tweets.get(friend)));
				}
				else {					
					context.write(new LongWritable(follower), 
							new IntWritable(0));
				}
			}
//			else {
//				if(map_user_tweets.containsKey(friend)) {
//					context.write(new LongWritable(friend), 
//							new IntWritable(0));
//				}
//			}
		}
	}
	
	static class SumFriendsTweetsReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		
		/**
		 * The output of reduce function is with this format:
		 * 
		 * follower|total num of friends's tweets
		 */
		@Override
	    protected void reduce(LongWritable key, Iterable<IntWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
	    	
	    	int num_tweets = 0;
	    	for(IntWritable value : values){
	    		num_tweets += value.get();
	    	}
	    	context.write(key, new IntWritable(num_tweets));
	    }
	}
	
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(TwitterRankConstant.TWEET_BY_USER_PATH()));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
			
			String hdfs_dir = TwitterRankConstant.SUM_FRIENDS_TWEETS_PATH();
			Path outputDir = new Path(hdfs_dir);	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(graph_path));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(SumFriendsTweetsHadoop.class);
			job.setJobName("sum friends' tweets");
			job.setMapperClass(SumFriendsTweetsMapper.class);
			//job.setMapOutputKeyClass(LongWritable.class);
			//job.setMapOutputValueClass(IntWritable.class);
			
			//job.setCombinerClass(SumFriendsTweetsReducer.class);
			job.setReducerClass(SumFriendsTweetsReducer.class);
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
