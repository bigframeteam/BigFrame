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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;

/**
 * A class to compute the random suffer vector used in TwitterRank.
 * 
 * @author andy
 *
 */
public class RandSufferVectorHadoop extends HadoopJob {

	public RandSufferVectorHadoop(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}

	static class  RandSufferVectorMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		private Map<String, Integer> map_product_tweets = new HashMap<String, Integer>();
		
		@Override
		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();
			Path[] uris = DistributedCache.getLocalCacheFiles(mapred_config);
			   
			for(Path p : uris) {  		   
				if (p.toString().contains("part")) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					while (line != null) {
						String [] fields = line.split("\\|");
			
						String itemSK = fields[0];
						String num_tweets = fields[1];
			
						if (!itemSK.equals("") && !num_tweets.equals("")) {
							map_product_tweets.put(itemSK, Integer.parseInt(num_tweets));
						}
						line = in.readLine();
					}
					in.close();
				}
			}
			
		}
		
		/**
		 * The map output is with this format:
		 * 
		 * itemSK|userID|probability
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			

			String[] fields = value.toString().split("\\|");
			
			String userID = fields[0];
			String itemSK = fields[1];
			String count = fields[2];
			
			if(!count.equals("")) {
				int num_tweets = Integer.parseInt(count);
				
				context.write(new Text(itemSK + "|" + userID), 
						new FloatWritable(num_tweets*1.0f/map_product_tweets.get(itemSK)));
			}
			
		}
	}
	
	
	@Override
	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(TwitterRankConstant.TWEET_BY_PRODUCT_PATH()));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
			
			String hdfs_dir = TwitterRankConstant.RAND_SUFFER_VECTOR();
			Path outputDir = new Path(hdfs_dir);	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(SenAnalyzeConstant.FILTERED_TWEETS_PATH()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass( RandSufferVectorHadoop.class);
			job.setJobName("Compute random suffer vector");
			job.setMapperClass( RandSufferVectorMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
			job.setNumReduceTasks(0);
			
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
