package bigframe.workflows.BusinessIntelligence.text.exploratory;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;

/**
 * A Hadoop job for filtering the tweets that didn't mention any promoted products. 
 * @author andy
 *
 */
public class FilterTweetHadoop extends HadoopJob{
	String promotedProd_path = ReportSalesConstant.PROMOTED_PROD_PATH();
	String tweet_path;
	
	public FilterTweetHadoop(String tweet_path, Configuration mapred_config) {
		super(mapred_config);
		this.tweet_path = tweet_path;
	}
	
	/**
	 * A mapper class to do a map-side filtering by the promoted products.
	 *  
	 * @author andy
	 *
	 */
	static class FilterTweetMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		private List<Long> itemSKs = new ArrayList<Long>();
		private List<String> productNames = new ArrayList<String>();
		
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
						String productName = fields[1];
			
						if (!itemSK.equals("") && !productName.equals("")) {
							itemSKs.add(Long.parseLong(itemSK));
							productNames.add(productName);
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
	  	  
	  	  	JSONParser parser = new JSONParser();
			Object obj;
			try {
				obj = parser.parse(value.toString());
				JSONObject jsonObject = (JSONObject) obj;
				
				String text = (String) jsonObject.get("text");
				
				JSONObject user_json = (JSONObject) jsonObject.get("user");
				Long user_id = (Long) user_json.get("id");
				
				JSONObject entities_json = (JSONObject) jsonObject.get("entities");
				JSONArray hashtags = (JSONArray) entities_json.get("hashtags");
				
				
				for(Object tag : hashtags) {
					String tag_str = (String) tag;
					
					for(int i = 0; i < productNames.size(); i++) {
						if(tag_str.equals(productNames.get(i))) {
							context.write(new Text(user_id + "|" + itemSKs.get(i)), new Text(text));
						}
					}
				}
				
			} catch (ParseException e) {
				e.printStackTrace();
			}					

		}
	}
	
	/**
	 * Aggregate the number of tweets that a user mentioned for a specific product. 
	 * @author andy
	 *
	 */
	static class FilterTweetReducer extends Reducer<Text, Text, Text, Text> {
		
		
		/**
		 * The reduce output is with this format:
		 * 
		 * userID|itemSK|num of tweets|the aggregated tweet text
		 */
		@Override
	    protected void reduce(Text key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
	    	
			StringBuilder sb = new StringBuilder();
	    	
	    	Integer size = 0;
	    	for(Text value : values){
	    		size++;
	    		sb.append(value.toString());
	    	}
	    	
	    	context.write(key, new Text(size.toString() + "|" + sb.toString()));
	    }
	}
	



	@Override
	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(promotedProd_path));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
		
			String hdfs_dir = SenAnalyzeConstant.FILTERED_TWEETS_PATH();
			Path outputDir = new Path(hdfs_dir);	
		
			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(tweet_path));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(FilterTweetHadoop.class);
			job.setJobName("filter tweets by promoted product");
			job.setMapperClass(FilterTweetMapper.class);
			job.setReducerClass(FilterTweetReducer.class);
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
