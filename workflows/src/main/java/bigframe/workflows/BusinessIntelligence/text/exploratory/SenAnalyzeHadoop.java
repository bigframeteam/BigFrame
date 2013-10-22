package bigframe.workflows.BusinessIntelligence.text.exploratory;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;
import bigframe.workflows.util.SenExtractorEnum;
import bigframe.workflows.util.SenExtractorFactory;
import bigframe.workflows.util.SentimentExtractor;

/**
 * A class for doing sentiment analysis on tweets.
 * 
 * @author andy
 *
 */
public class SenAnalyzeHadoop extends HadoopJob {
	String tweet_path;
	
	
	public SenAnalyzeHadoop(String tweet_path, Configuration mapred_config) {
		super(mapred_config);
		this.tweet_path = tweet_path;
	}

	static class SenAnalyzeMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		private SentimentExtractor senAnalyst  = SenExtractorFactory.getSenAnalyze(SenExtractorEnum.SIMPLE);
	  	private JSONParser parser = new JSONParser();
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {		
			try {
				Object obj;
				obj = parser.parse(value.toString());
				JSONObject jsonObject = (JSONObject) obj;
					
				String text = (String) jsonObject.get("text");
					
				JSONObject entities_json = (JSONObject) jsonObject.get("entities");
				JSONArray hashtags = (JSONArray) entities_json.get("hashtags");
				
				for(Object tag : hashtags) {
					String tag_str = (String) tag;
					context.write(new Text(tag_str), new FloatWritable(senAnalyst.getSentiment(text)));
				}
					
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}					

		}
	}
	
	static class SenAnalyzeMapper2 extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		private SentimentExtractor senAnalyst  = SenExtractorFactory.getSenAnalyze(SenExtractorEnum.SIMPLE);
	  	private JSONParser parser = new JSONParser();
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						
			String [] fields = value.toString().split("\\|");
			
			String userID = fields[0];
			String itemSK = fields[1];
			String text = fields[3];
			
			context.write(new Text(itemSK + "|" + userID), new FloatWritable(senAnalyst.getSentiment(text)));
			
		}
	}
	
	/**
	 * Aggregate the sentiment scores.
	 * 
	 * @author andy
	 *
	 */
	static class SenAnalyzeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	    
		/**
		 * The output of the reduce is with this format:
		 * 
		 * if job input is raw tweets:
		 * 		Product names|sentiment score
		 * else if job input is filtered tweets:
		 * 		itemSK|userID|sentiment score
		 */
				
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
	    	Float sum_sentiment = 0f;
	    	
	    	for(FloatWritable value : values) {
	    		sum_sentiment = value.get();
	    	}
	    	context.write(key, new FloatWritable(sum_sentiment));
	    }
	}
	
	
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);
		
			String hdfs_dir = SenAnalyzeConstant.REPORT_SENTIMENT();
			Path outputDir = new Path(hdfs_dir);	
		


			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(tweet_path));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(SenAnalyzeHadoop.class);
			job.setJobName("sentiment analysis");
			
			if(tweet_path.equals(SenAnalyzeConstant.FILTERED_TWEETS_PATH())) {
				job.setMapperClass(SenAnalyzeMapper2.class);
			}
			else
				job.setMapperClass(SenAnalyzeMapper1.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(FloatWritable.class);
			
			job.setCombinerClass(SenAnalyzeReducer.class);
			job.setReducerClass(SenAnalyzeReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
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


//	@Override
//	public Boolean call() {
//		// TODO Auto-generated method stub
//		return run();
//	}


}


