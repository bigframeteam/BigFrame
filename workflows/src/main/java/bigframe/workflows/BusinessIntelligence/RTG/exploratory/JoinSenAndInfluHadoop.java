package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeHadoop;


/**
 * Join the sentiment score and the user influence computed by TwitterRank.
 * 
 * @author andy
 *
 */
public class JoinSenAndInfluHadoop extends HadoopJob {

//	private static String TWITTER_RANK = "TR";
//	private static String SEN_SCORE = "SC";
	
	public JoinSenAndInfluHadoop(Configuration _mapred_config) {
		super(_mapred_config);
		// TODO Auto-generated constructor stub
	}

	static class JoinSenAndInfluMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {	
			
			String [] fields = value.toString().split("\\|");
			
			context.write(new Text(fields[0] + "|" + fields[1]),
						new FloatWritable(Float.parseFloat(fields[2])));
		}
	}
	
	static class JoinSenAndInfluReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			float ans = 1;
			
			for(FloatWritable value : values) {
				ans *= value.get();
			}
			
			context.write(key, new FloatWritable(ans));
		}
	}
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		// TODO Auto-generated method stub
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);
		
			String hdfs_dir = TwitterRankConstant.JOIN_SEN_INFLUENCE();
			Path outputDir = new Path(hdfs_dir);	
		

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			
			FileStatus[] status1 = fs.listStatus(new Path(TwitterRankConstant.TWITTER_RANK() ));
			
			for (FileStatus stat1 : status1){
				if(stat1.getPath().toString().contains("product")) {
					FileStatus[] status2 = fs.listStatus(stat1.getPath());
					
					for(FileStatus stat2 : status2) {
						FileInputFormat.addInputPath(job, stat2.getPath());
					}
				}

			}
			
			FileInputFormat.addInputPath(job, new Path(SenAnalyzeConstant.REPORT_SENTIMENT()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(JoinSenAndInfluHadoop.class);
			job.setJobName("join sentiment and influence score");
			

			job.setMapperClass(JoinSenAndInfluMapper.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(FloatWritable.class);
			
			//job.setCombinerClass(SenAnalyzeReducer.class);
			job.setReducerClass(JoinSenAndInfluReducer.class);
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

}
