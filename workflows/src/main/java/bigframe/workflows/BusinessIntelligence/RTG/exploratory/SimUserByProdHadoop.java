package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * A class to compute the similarity between user for a specific products.
 * It is used by the TwitterRank algorithm.
 * 
 * @author andy
 *
 */
public class SimUserByProdHadoop extends HadoopJob {

	private String graph_path;
	
	private static String USER_PAIR = "user_pair";
	private static String GRAPH = "graph";
	
	public SimUserByProdHadoop(String graph_path, Configuration mapred_config) {
		super(mapred_config);
		this.graph_path = graph_path;
	}

	static class SimUserByProdMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathname = fileSplit.getPath().toString();
			
			if(pathname.contains(TwitterRankConstant.USERPAIR_PROD_PROB_())) {
				String [] fields = value.toString().split("\\|");
				
				String itemSK = fields[0];
				String twitterID1 = fields[1];
				String twitterID2 = fields[2];
				String prob1 = fields[3];
				String prob2 = fields[4];
				
				context.write(new Text(twitterID1 + "|" + twitterID2), 
						new Text(USER_PAIR + "|" + itemSK + "|" + prob1 + "|" + prob2));
				
				if(!twitterID1.equals(twitterID2)) {
					context.write(new Text(twitterID2 + "|" + twitterID1), 
							new Text(USER_PAIR + "|" + itemSK + "|" + prob2 + "|" + prob1));
				}
			}
			else {
				String [] fields = value.toString().split("\\|");
				
				String friend = fields[0];
				long follower = Long.parseLong(fields[1]);
				
				context.write(new Text(follower + "|" + friend), 
						new Text(GRAPH));
			}

		}
	}
	
	static class SimUserByProdReducer extends Reducer<Text, Text, Text, FloatWritable> {
	    
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {

			Boolean isFriend = false;
			List<String> pair_list = new LinkedList<String>();
			for(Text value : values) {
				String [] fields = value.toString().split("\\|");
				if(fields[0].equals(GRAPH)) {
					isFriend = true;
				}
				else
					pair_list.add(value.toString());
			}
			
			if(isFriend) {
				for(String value : pair_list) {
					String [] fields = value.toString().split("\\|");
					String itemSK = fields[1];
					float prob1 = Float.parseFloat(fields[2]);
					float prob2 = Float.parseFloat(fields[3]);
					

					context.write(new Text(key + "|" + itemSK), 
							new FloatWritable(1 - Math.abs(prob1 - prob2)));

				}
			}
			else {
				String [] users = key.toString().split("\\|");
				
				if(users[0].equals(users[1]))
					for(String value : pair_list) {
						String [] fields = value.toString().split("\\|");
						String itemSK = fields[1];
						
	
						context.write(new Text(key + "|" + itemSK), 
								new FloatWritable(0));
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
		
			String hdfs_dir = TwitterRankConstant.SIMILARITY_B_USERS();
			Path outputDir = new Path(hdfs_dir);	
		

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.addInputPath(job, new Path(TwitterRankConstant.USERPAIR_PROD_PROB_()));
			FileInputFormat.addInputPath(job, new Path(graph_path));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(SimUserByProdHadoop.class);
			job.setJobName("Compute similarity between user");
			job.setMapperClass(SimUserByProdMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(SimUserByProdReducer.class);
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
