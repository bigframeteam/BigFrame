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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import bigframe.bigif.WorkflowInputFormat;


public class TweetParser {
	
	public static class TweetParserMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
	
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
	
			JSONParser parser = new JSONParser();
	
			SimpleDateFormat tweetFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
			tweetFormat.setLenient(true);
	
			try {
	
				JSONObject tweet_json = (JSONObject) parser.parse(line);
				JSONObject user_json = (JSONObject) tweet_json.get("user");

				String tweet_id = (String) tweet_json.get("id");
				String text = (String) tweet_json.get("text");
				String create_at_str = (String) tweet_json.get("created_at");
	
				Long user_id = (Long) user_json.get("id");
	
				Timestamp create_at = new Timestamp(tweetFormat.parse(create_at_str).getTime());
				String date = create_at.toString();
				date = date.substring(0, date.length() - 2);
				String output = tweet_id + "|" + user_id + "|" + date + "|" + text;
	
				context.write(new Text(output), NullWritable.get());
	
			} catch (org.json.simple.parser.ParseException | ParseException e1) {
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

			Job job = new Job(conf, "tweet parse");
		
			job.setNumReduceTasks(0);
		
			job.setJarByClass(TweetParser.class);
			job.setMapperClass(TweetParserMapper.class);
		
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		
			FileInputFormat.addInputPath(job, new Path(hdfsPath));
			String outputPath = workIF.getHDFSRootDIR() + "/parsed_tweet";
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