package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;

/**
 * A class to compute the transit matrix for Twitter Rank algorithm.
 * 
 * TO DO: need to implement a customized partitioner.
 *  
 * @author andy
 *
 */
public class TransitMatrixHadoop extends HadoopJob {


	private String graph_path;
	
	private static String SIM_BETWEEN_USER = "sim_between_users";
	private static String SUM_FRIENDS_TWEETS = "sum_friends_tweets";
	private static String FRIEND_TWEETS = "friend_tweets";
	
	public TransitMatrixHadoop(String graph_path, Configuration mapred_config) {
		super(mapred_config);
		this.graph_path = graph_path;
	}

	static class TransitMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
		
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
	    				String num_tweets =	fields[1];

	    				if (!twitterID.equals("") && !num_tweets.equals("")) {
	    					map_user_tweets.put(twitterID, Integer.parseInt(num_tweets));
	    				}
	    				line = in.readLine();
	  	   			}
	  	   			in.close();
	  	   		}
	  	   		
	  	   	}
		}
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathname = fileSplit.getPath().toString();
	  	   	
			
			if(pathname.contains( TwitterRankConstant.SIMILARITY_B_USERS() )) {
				String [] fields = value.toString().split("\\|");
				
				String follower = fields[0];
				String friend = fields[1];
				String itemSK = fields[2];
				String similarity = fields[3];
				
				System.out.println(value);
				
				context.write(new Text(follower), 
						new Text(SIM_BETWEEN_USER + "|" + friend + "|" + itemSK + "|" + similarity));
			}
			else if (pathname.contains( TwitterRankConstant.SUM_FRIENDS_TWEETS_PATH() )){
				String [] fields = value.toString().split("\\|");
				
				String follower = fields[0];
				String num_tweets = fields[1];

				context.write(new Text(follower), 
						new Text(SUM_FRIENDS_TWEETS + "|" + num_tweets));
			}
			else {
				String [] fields = value.toString().split("\\|");
				
				String friend = fields[0];
				String follower = fields[1];
				
				if(map_user_tweets.containsKey(follower)) {
					if(map_user_tweets.containsKey(friend)) {
						context.write(new Text(follower), 
								new Text(FRIEND_TWEETS + "|" + friend + "|" + map_user_tweets.get(friend)));
					}
					else
						context.write(new Text(follower), 
								new Text(FRIEND_TWEETS + "|" + friend + "|" + 0) );
				}
			}

		}
	}
	
	static class TransitMatrixReducer extends Reducer<Text, Text, Text, Text> {
		
		
		/**
		 * The reduce output is with this format:
		 * 
		 * itemSK|follower|friend|probability
		 */
		@Override
	    protected void reduce(Text key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
	    	
			int sum_friends_tweets = 0;
			Map<String, Integer> map_friend_tweets = new HashMap<String, Integer>();
			//Map<String, Float> map_friend_similarity = new HashMap<String, Float>();
			
			List<String> transit_cells = new LinkedList<String>();
			
			for(Text value : values) {
				String [] fields = value.toString().split("\\|");
				
				if(fields[0].equals(SIM_BETWEEN_USER)) {
					//map_friend_similarity.put(fields[1], Float.parseFloat(fields[3]));
					transit_cells.add(value.toString());
				}
				
				else if(fields[0].equals(SUM_FRIENDS_TWEETS)) {
					sum_friends_tweets = Integer.parseInt(fields[1]);
				}
				
				else if(fields[0].equals(FRIEND_TWEETS)) {
					map_friend_tweets.put(fields[1], Integer.parseInt(fields[2]));
				}
			}
			
			if(sum_friends_tweets != 0) {
				for(String cell : transit_cells) {
					String [] fields = cell.toString().split("\\|");
					
					String friend = fields[1];
					String itemSK = fields[2];
					float similarity = Float.parseFloat(fields[3]);
					
					
					String follower = key.toString();
					if(!follower.equals(friend)) 
						context.write(new Text(itemSK), 
								new Text(key + "|" + friend + "|" +(map_friend_tweets.get(friend)*similarity)/sum_friends_tweets));
				}
			}
			// Follower should be equal to Friend.
			else {
				assert(transit_cells.size() == 1);
				for(String cell : transit_cells) {
					String [] fields = cell.toString().split("\\|");
					
					String friend = fields[1];
					String itemSK = fields[2];
					
					context.write(new Text(itemSK ), 
							new Text(key + "|" + friend + "|" + 0));
				}
			}
	    }
	}
	
	
	@Override
	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(TwitterRankConstant.TWEET_BY_USER_PATH()));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
			
			String hdfs_dir = TwitterRankConstant.TRANSIT_MATRIX();
			Path outputDir = new Path(hdfs_dir);	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.addInputPath(job, new Path(graph_path));
			FileInputFormat.addInputPath(job, new Path(TwitterRankConstant.SIMILARITY_B_USERS()));
			FileInputFormat.addInputPath(job, new Path(TwitterRankConstant.SUM_FRIENDS_TWEETS_PATH()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(TransitMatrixHadoop.class);
			job.setJobName("Compute transit Matrix");
			job.setMapperClass(TransitMatrixMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			//job.setCombinerClass(TransitMatrixReducer.class);
			job.setReducerClass(TransitMatrixReducer.class);
			job.setOutputKeyClass(Text.class);
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
