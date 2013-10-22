package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
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
 * A class to compute the probabilities of every pair of users when talking about a 
 * specific product.
 * 
 * It is used for compute the transit matrix.
 * @author andy
 *
 */
public class UserPairProbHadoop extends HadoopJob {

	private String tweet_by_user = TwitterRankConstant.TWEET_BY_USER_PATH();
	private String filter_tweets= SenAnalyzeConstant.FILTERED_TWEETS_PATH();
	
	public UserPairProbHadoop(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}

	

	static class UserPairProbMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private static final Log LOG = LogFactory.getLog(UserPairProbMapper.class);
		
		private Map<String, Integer> map_user_tweets = new HashMap<String, Integer>();
		
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
	    				String num_tweets = fields[1];

	    				if (!twitterID.equals("") && !num_tweets.equals("")) {
	    					map_user_tweets.put(twitterID, Integer.parseInt(num_tweets));
	    				}
	    				else {
	    					LOG.info("Contains empty field in the record!");
	    				}
	    			
	    				line = in.readLine();
	  	   			}
	  	   			in.close();
	  	   		}
	  	   		
	  	   	}
		}
		
		/**
		 * The map input is with this format:
		 * 
		 * userID|itemSK|num of tweets|the aggregated tweet text
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {
	  	   	
	  	   	String [] fields = value.toString().split("\\|");
	  	   	
	  	   	String twitterID = fields[0];
	  	   	String itemSK = fields[1];
	  	   	Float num_tweets = Float.parseFloat(fields[2]);
	  	   	
	  	   	if(map_user_tweets.containsKey(twitterID)) 
	  	   		context.write(new LongWritable(Long.parseLong(itemSK)), 
	  	   				new Text(twitterID + "|" + num_tweets/map_user_tweets.get(twitterID)));
	  	   	else
	  	   		LOG.warn("Twitter ID: " + twitterID + " in filtered tweets not exist in tweet by user");
	  	   	
		}
	}
	
	static class UserPairProbReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	    
		/**
		 * The reduce output is with this format:
		 * 
		 * itemSK|twitterID1|twitterID2|probability1|probability2
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
//			System.out.println(key);
//			System.out.println("----------------------------------");
			List<String> value_list = new LinkedList<String>();
			for(Text value : values) {
//				System.out.println(value);
				value_list.add(value.toString());
			}
//			System.out.println("----------------------------------");
//					 	
//			for(String value : value_list) {
//				System.out.println(value);
//			}
//			System.exit(1);
			
			for(int i = 0; i < value_list.size(); i++) {
				String [] fields1 = value_list.get(i).toString().split("\\|");
				String twitterID1 = fields1[0];
				Float prob1 = Float.parseFloat(fields1[1]);
				System.out.println("Twitter ID 1:" + twitterID1);
				for(int j = i; j < value_list.size(); j++) {
					String [] fields2 = value_list.get(j).toString().split("\\|");
					String twitterID2 = fields2[0];
					Float prob2 = Float.parseFloat(fields2[1]);
					System.out.println("Twitter ID 2:" + twitterID2);
					context.write(key, new Text(twitterID1 + "|" + twitterID2 + "|" + prob1 + "|" + prob2));
				}
			}
			
			
	    }
	}
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(tweet_by_user));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}

		
			String hdfs_dir = TwitterRankConstant.USERPAIR_PROD_PROB_();
			Path outputDir = new Path(hdfs_dir);	
		


			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(filter_tweets));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(UserPairProbHadoop.class);
			job.setJobName("Compute user pair probability");
			job.setMapperClass(UserPairProbMapper.class);
			
			job.setReducerClass(UserPairProbReducer.class);
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
