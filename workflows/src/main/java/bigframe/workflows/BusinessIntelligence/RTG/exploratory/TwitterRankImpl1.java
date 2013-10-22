package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;


/**
 * A twitter rank implementation based on output of 
 * {@link bigframe.queries.BusinessIntelligence.RTG.exploratory.SpliByProdHadoopImpl1}.
 * 
 * @author andy
 *
 */
public class TwitterRankImpl1 extends TwitterRankHadoop {
	
	private static String ITERATION = "iteration";

	public TwitterRankImpl1(int num_iter, Configuration mapred_config) {
		super(num_iter, mapred_config);
		// TODO Auto-generated constructor stub
	}

	/**
	 * A class to run the twitter rank algorithm for a single product.
	 * 
	 * @author andy
	 *
	 */
	private class SingleTwitterRank implements Runnable {
		
		Job job;
		Path product_path;
		FileSystem fs;
		
		public SingleTwitterRank(Job job, Path prod_path, FileSystem fs) {
			this.job = job;
			this.product_path = prod_path;
			this.fs = fs;
		}
		
		public void run() {
			Integer iteration = 0;
			
			for(int i = 0; i < num_iter; i++) {		
				try {
					FileStatus[] status = fs.listStatus(new Path(product_path.toString() + "/" + TwitterRankConstant.RAND_SUFFER_VECTOR()));
					
					for (FileStatus stat : status){
						DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
							job.getConfiguration());
					}
					
					job.getConfiguration().setInt(ITERATION, iteration);
					
					String hdfs_dir = product_path.toString() + "/" + TwitterRankImpl1.ITERATION + iteration;
					Path outputDir = new Path(hdfs_dir);	
					
					if(fs.exists(outputDir))
						fs.delete(outputDir, true);
	
					// Add the rankings after each iteration to distributed cached.
					if(iteration == 0) {
						//FileInputFormat.addInputPath(job, new Path(product_path.toString() + "/" + TwitterRankConstant.INITIAL_RANK()));
						status = fs.listStatus(new Path(product_path.toString() + "/" + TwitterRankConstant.INITIAL_RANK()));
						
						for (FileStatus stat : status){
							DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
								job.getConfiguration());
						}
					}
					else {
						//FileInputFormat.addInputPath(job, new Path(product_path.toString() + "/" + TwitterRankImpl1.ITERATION + (iteration-1)) );
						status = fs.listStatus(new Path(product_path.toString() + "/" + TwitterRankImpl1.ITERATION + (iteration-1) ));
						
						for (FileStatus stat : status){
							DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
								job.getConfiguration());
						}
					}
					
					// Also broadcast the random shuffle vector.
					status = fs.listStatus(new Path(product_path.toString() + "/" + TwitterRankConstant.RAND_SUFFER_VECTOR() ));
					
					for (FileStatus stat : status){
						DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
							job.getConfiguration());
					}
					
			
					FileInputFormat.setInputPaths(job, new Path(product_path.toString() + "/" + TwitterRankConstant.TRANSIT_MATRIX()));
					
					FileOutputFormat.setOutputPath(job, outputDir);
					
					job.setJarByClass(TwitterRankImpl1.class);
					job.setJobName("Twitter rank");
					job.setMapperClass(TwitterRankImpl1Mapper.class);
					job.setMapOutputKeyClass(LongWritable.class);
					job.setMapOutputValueClass(Text.class);
					
					//job.setCombinerClass(TwitterRankImpl1Reducer.class);
					job.setReducerClass(TwitterRankImpl1Reducer.class);
					job.setOutputKeyClass(LongWritable.class);
					job.setOutputValueClass(FloatWritable.class);
					
					job.setNumReduceTasks(1);
						
					job.waitForCompletion(true);
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	
	static class TwitterRankImpl1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						

			
			String [] fields = value.toString().split("\\|");
			
			
			// The transit matrix has followers as row and friends as column
			Long followerID = Long.parseLong(fields[1]);
			
			context.write(new LongWritable(followerID), new Text(fields[2] + "|" + fields[3]));

		}
	}
	
	static class TwitterRankImpl1Reducer extends Reducer<LongWritable, Text, LongWritable, FloatWritable> {
		
		// map for the user rankings. (user:rank).
		private Map<Long, Float> map_ranks = new HashMap<Long, Float>();
		
		// map for the random suffer vector. (user:random jump probability).
		private Map<Long, Float> map_RSV = new HashMap<Long, Float>();
		
		// keep track of the iteration the algorithm has run.
		private Integer iteration;
		
		@Override
		protected void setup(final Context context) throws IOException {

			Configuration mapred_config = context.getConfiguration();
			iteration = mapred_config.getInt(TwitterRankImpl1.ITERATION, 0);
			
			Path[] uris = DistributedCache.getLocalCacheFiles(mapred_config);
			   
			for(Path p : uris) {  		   
				if (p.toString().contains("part") && p.toString().contains(TwitterRankConstant.INITIAL_RANK())) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					while (line != null) {
						String [] fields = line.split("\\|");
			
						String userID = fields[1];
						String rank = fields[2];
			
						if (!userID.equals("") && !rank.equals("")) {
							map_ranks.put(Long.parseLong(userID), Float.parseFloat(rank));
						}
						line = in.readLine();
					}
					in.close();
				}
				
				else if (p.toString().contains("part") && p.toString().contains(TwitterRankConstant.RAND_SUFFER_VECTOR()) ) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					while (line != null) {
						String [] fields = line.split("\\|");
			
						String userID = fields[1];
						String jump_prob = fields[2];
			
						if (!userID.equals("") && !jump_prob.equals("")) {
							map_RSV.put(Long.parseLong(userID), Float.parseFloat(jump_prob));
						}
						line = in.readLine();
					}
					in.close();
				
				}
				else if (p.toString().contains("part") && p.toString().contains(TwitterRankImpl1.ITERATION)) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					while (line != null) {
						String [] fields = line.split("\\|");
			
						String userID = fields[0];
						String rank = fields[1];
			
						if (!userID.equals("") && !rank.equals("")) {
							map_ranks.put(Long.parseLong(userID), Float.parseFloat(rank));
						}
						line = in.readLine();
					}
					in.close();
				}
			}
		}
		
		
		@Override
	    protected void reduce(LongWritable key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			float new_rank = 0;
			float alpha = 0.85f;
			
			for (Text value : values) {
				String [] fields = value.toString().split("\\|");
				
				String friend = fields[1];
				Float transit_prob = Float.parseFloat(fields[2]);
				
				new_rank += alpha * transit_prob * map_ranks.get(friend) + (1 - alpha) * map_RSV.get(friend);
				
			}
			
			context.write(key, new FloatWritable(new_rank));
	    }
	}


	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			final FileSystem fs = FileSystem.get(mapred_config);
				
			
			FileStatus[] status = fs.listStatus(new Path(TwitterRankConstant.TWITTER_RANK()));
			//Set<String> promoted_prod = new HashSet<String>();
			

			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
			
			// Run TwitterRank algorithm for each product.
			for (FileStatus stat : status){
				Path product_path = stat.getPath();
				Job job = new Job(mapred_config);
								
				SingleTwitterRank tr = new SingleTwitterRank(job, product_path, fs);
				tr.run();								
			}			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	
}
