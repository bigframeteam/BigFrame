package bigframe.util.dataloader.hana;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import bigframe.bigif.WorkflowInputFormat;


public class EntitiesParser {
	
	public static class EntitiesParserMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
	
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			JSONParser parser = new JSONParser();
			
			try {

				JSONObject tweet_json = (JSONObject) parser.parse(line);
				String tweet_id = (String) tweet_json.get("id");

	            JSONObject entities_json = (JSONObject) tweet_json.get("entities");
	            JSONArray hashtags = (JSONArray) entities_json.get("hashtags");
	            
	            if (!hashtags.isEmpty()) {
	            	for(Object tag : hashtags) {
	            		String output = tweet_id + "|" + (String)tag;
	            		context.write(new Text(output), NullWritable.get());
	            	}
	            }
	            else {
	            	String output = tweet_id + "|";
	            	context.write(new Text(output), NullWritable.get());
	            }
	            
	            
			} catch (org.json.simple.parser.ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	public boolean run(String hdfsPath, WorkflowInputFormat workIF) {
		Configuration conf = new Configuration();
		conf.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/core-site.xml"));
		conf.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/mapred-site.xml"));
		
		try {

			Job job = new Job(conf, "entities parse");
		
			job.setNumReduceTasks(0);
		
			job.setJarByClass(EntitiesParser.class);
			job.setMapperClass(EntitiesParserMapper.class);
		
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		
			FileInputFormat.addInputPath(job, new Path(hdfsPath));
			String outputPath = workIF.getHDFSRootDIR() + "/parsed_entities";
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
			return job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}