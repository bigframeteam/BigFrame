package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TwitterRankImpl extends TwitterRankHadoop {

	private static final String TRANSIT_MATRIX = "0";
	private static final String RANKING = "1";
	private static final String RANDOM_SUFFER_VEC = "2";
	private static final String TRANSIT_MATRIX_RANKING = "3";
	private static final String ITERATION = "iteration";
	
	private static final float ALPHA = 0.85f;
	
	public TwitterRankImpl(int num_iter, Configuration mapred_config) {
		super(num_iter, mapred_config);
		// TODO Auto-generated constructor stub
	}

	static class TranRankReduceJoinMapper  extends Mapper<LongWritable, Text, Text, Text> {
		
		private boolean isFromTranMat;
		
		@Override
		protected void setup(final Context context) throws IOException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathname = fileSplit.getPath().toString();
			
			if(pathname.contains(TwitterRankConstant.TRANSIT_MATRIX())) {
				isFromTranMat = true;
			}
			
			else
				isFromTranMat = false;
			
		}
		
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						

//			FileSplit fileSplit = (FileSplit) context.getInputSplit();
//			String pathname = fileSplit.getPath().toString();
			
			if(isFromTranMat) {
				String [] fields = value.toString().split("\\|");
				
				//Key = (itemsk|friend_id), value = (0|follower_id|prob)
				context.write(new Text(fields[0] + "|" + fields[2]), new Text(TRANSIT_MATRIX + "|" 
						+ fields[1] + "|" + fields[3]));			
			}
			else {
				String [] fields = value.toString().split("\\|");

				//Key = (itemsk|user_id), value = (1|ranking score)
				context.write(new Text(fields[0] + "|" + fields[1]), new Text(RANKING + "|" 
						+ fields[2]));	
			}		

		}

	}
	
	
	
	static class TranRankReduceJoinReducer extends Reducer<Text, Text, Text, FloatWritable> {
	
//		class TranMatrixRecord {
//			public long follower_id;
//		}
		
		/**
		 * The reduce output is with this format:
		 * 
		 * itemSK|userID|influence score
		 */
		@Override
	    protected void reduce(Text key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			String itemSK = key.toString().split("\\|")[0];
			String user_id = key.toString().split("\\|")[1];
			
			List<String> Transit_matrix = new LinkedList<String>();
			List<String> Ranking = new LinkedList<String>();
			
			for(Text value : values) {
				if(value.toString().split("\\|")[0].equals(TRANSIT_MATRIX)) {
					Transit_matrix.add(value.toString());
				}
				
				else
					Ranking.add(value.toString());
			}
			
			for(String cell1 : Transit_matrix) {
				String [] fields1 = cell1.split("\\|");
				
				String followID = fields1[1];
				float prob = Float.parseFloat(fields1[2]);
				if(!followID.equals(user_id)) {
							
					for(String cell2 : Ranking) {
						String [] fields2 = cell2.split("\\|");
							
						// Key = (itemSK|followerID), value = ( prob * rank score)
						context.write(new Text(itemSK + "|" + followID), 
								new FloatWritable(prob * Float.parseFloat(fields2[1])));
					}
				}
			}			
	    }
	}

	static class TranRankGroupMapper  extends Mapper<LongWritable, Text, Text, FloatWritable> {
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						

			String [] fields = value.toString().split("\\|");
			
			context.write(new Text(fields[0] + "|" + fields[1]), 
					new FloatWritable(Float.parseFloat(fields[2])) );

		}

	}
	
	static class TranRankGroupReducer  extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		@Override
	    protected void reduce(Text key, Iterable<FloatWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			float sum = 0;
			
			for(FloatWritable value : values) {
				sum += value.get();
			}
			
			// Key = (itemSK|followerID), value = ( sum (prob * rank score) )
			context.write(key, new FloatWritable(sum));
		}

	}
	
	
	static class ReduceSideJoinRandMapper  extends Mapper<LongWritable, Text, Text, Text> {
		private boolean isFromRandSuff;
		
		@Override
		protected void setup(final Context context) throws IOException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathname = fileSplit.getPath().toString();
			
			if(pathname.contains(TwitterRankConstant.RAND_SUFFER_VECTOR())) {
				isFromRandSuff = true;
			}
			
			else
				isFromRandSuff = false;
			
		}
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String pathname = fileSplit.getPath().toString();
			
			String [] fields = value.toString().split("\\|");
			
			if(isFromRandSuff) {
				context.write(new Text(fields[0] + "|" +fields[1]), new Text(RANDOM_SUFFER_VEC + "|" +
						fields[2]));
			}
			
			else {
				context.write(new Text(fields[0] + "|" +fields[1]), new Text(TRANSIT_MATRIX_RANKING + "|" +
						fields[2]));
			}
					
		}

	}
	
	static class ReduceSideJoinRandReducer  extends Reducer<Text, Text, Text, FloatWritable> {
		
		@Override
	    protected void reduce(Text key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			float rand_suffer_prob = 0;
			float ranking_score = 0;
			
			for(Text value : values) {
				String [] fields  = value.toString().split("\\|");
				
				if(fields[0].equals(RANDOM_SUFFER_VEC)) {
					rand_suffer_prob = Float.parseFloat(fields[1]);
				}
				else
					ranking_score =  Float.parseFloat(fields[1]);
			}
			
			// key = (itemSK|userID), value = (ranking score)
			context.write(key, new FloatWritable(ALPHA * ranking_score + (1-ALPHA) * rand_suffer_prob));
		}

	}
	
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		Boolean flag = true;
		
		Path parrent_path = new Path(TwitterRankConstant.TWITTER_RANK());
		
		try {
		
			FileSystem fs;
		
			fs = FileSystem.get(mapred_config);
	
			Configuration mapred_config_copy1 = new Configuration(mapred_config);
			mapred_config_copy1.set("mapred.textoutputformat.separator", "|");	
			
			if(fs.exists(parrent_path)) {
				fs.delete(parrent_path, true);
				fs.mkdirs(parrent_path);
			}
		
			Path temp_output1 = new Path(TwitterRankConstant.TWITTER_RANK()+ "/temp1");
			Path temp_output2 = new Path(TwitterRankConstant.TWITTER_RANK()+ "/temp2");
		
			for(int iter = 0; iter < num_iter; iter++) {		
			
				Job job1 = new Job(mapred_config_copy1);
				
				Path rank_input;
				Path rank_output = new Path(TwitterRankConstant.TWITTER_RANK()+ "/" + ITERATION + iter);
				
				if(iter == 0)
					rank_input = new Path(TwitterRankConstant.INITIAL_RANK());
				else
					rank_input = new Path(TwitterRankConstant.TWITTER_RANK()+ "/" + ITERATION + (iter-1) );
				
				if(iter == num_iter - 1) {
					rank_output = new Path(TwitterRankConstant.TWITTER_RANK()+ "/result");
				}
				
				FileInputFormat.addInputPath(job1, new Path(TwitterRankConstant.TRANSIT_MATRIX()));
				FileInputFormat.addInputPath(job1, rank_input);
				
				if(fs.exists(temp_output1)) {
					fs.delete(temp_output1, true);
				}
				
				FileOutputFormat.setOutputPath(job1, temp_output1);
				
				job1.setJarByClass(TwitterRankImpl.class);
				job1.setJobName("Twitter rank for iteration " + iter + 
						" Joining Transit Matrix and Random Suffer vector");
				job1.setMapperClass(TranRankReduceJoinMapper.class);
				job1.setMapOutputKeyClass(Text.class);
				job1.setMapOutputValueClass(Text.class);
				
				job1.setReducerClass(TranRankReduceJoinReducer.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(FloatWritable.class);
				
				job1.setNumReduceTasks(1);
				
				if(!job1.waitForCompletion(true)){
					flag = false;
					break;
				}
				
				Job job2 = new Job(mapred_config_copy1);
				FileInputFormat.addInputPath(job2, temp_output1);
				
				if(fs.exists(temp_output2)) {
					fs.delete(temp_output2, true);
				}
				
				FileOutputFormat.setOutputPath(job2, temp_output2);
//		
				job2.setJarByClass(TwitterRankImpl.class);
				job2.setJobName("Twitter rank for iteration " + iter + 
						" group rank score");
				job2.setMapperClass(TranRankGroupMapper.class);
				
				job2.setReducerClass(TranRankGroupReducer.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(FloatWritable.class);
				
				job2.setNumReduceTasks(1);
				
				if(!job2.waitForCompletion(true)){
					flag = false;
					break;
				}
				
				Job job3 = new Job(mapred_config_copy1);
				FileInputFormat.addInputPath(job3, temp_output2);
				FileInputFormat.addInputPath(job3, new Path(TwitterRankConstant.RAND_SUFFER_VECTOR()));
				
				
				FileOutputFormat.setOutputPath(job3, rank_output);
//		
				job3.setJarByClass(TwitterRankImpl.class);
				job3.setJobName("Twitter rank for iteration " + iter + 
						" compute the new rank");
				job3.setMapperClass(ReduceSideJoinRandMapper.class);
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(Text.class);
				
				job3.setReducerClass(ReduceSideJoinRandReducer.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(FloatWritable.class);
				
				job3.setNumReduceTasks(1);
				
				if(!job3.waitForCompletion(true)){
					flag = false;
					break;
				}
				
//				fs.delete(rank_input, true);
				

			
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return flag;
	}

}
