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
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;

import java.util.concurrent.Future;

import java.util.concurrent.Callable;


/**
 * A twitter rank implementation based on output of 
 * {@link bigframe.queries.BusinessIntelligence.RTG.exploratory.SpliByProdHadoopImpl2}.
 * 
 * @author andy
 *
 */
public class TwitterRankImpl2 extends TwitterRankHadoop {

	private static String ITERATION = "iteration";

	public TwitterRankImpl2(int num_iter, Configuration mapred_config) {
		super(num_iter, mapred_config);
		// TODO Auto-generated constructor stub
	}

	/**
	 * A class to run the twitter rank algorithm for a single product.
	 * 
	 * @author andy
	 *
	 */
	private class SingleTwitterRank implements Callable<Boolean> {
		
		Configuration mapred_config;
		FileSystem fs;
		Path intial_rank;
		Path rand_suffer_vec;
		Path transit_matrix;
		Path rank_output;
		
		public SingleTwitterRank(Configuration mapred_config, FileSystem fs, Path initial_rank, Path rand_suffer_vec, 
				Path transit_matrix, Path rank_output) {
			this.mapred_config = mapred_config;
			this.fs = fs;
			this.intial_rank = initial_rank;
			this.rand_suffer_vec = rand_suffer_vec;
			this.transit_matrix = transit_matrix;
			this.rank_output = rank_output;
		}
		
		public Boolean call() {		
			
			for(int iter = 0; iter < num_iter; iter++) {		
				try {
					
					Configuration mapred_config_copy = new Configuration(mapred_config);
					
					Job job = new Job(mapred_config_copy);
					
					// Put the random suffer vector into distributed cache
					DistributedCache.addCacheFile(new URI(rand_suffer_vec.toString()), 
							job.getConfiguration());
					
					
					job.getConfiguration().setInt(ITERATION, iter);
	
					// Add the rankings after each iteration to distributed cached.
					if(iter == 0) {
						DistributedCache.addCacheFile(new URI(intial_rank.toString()), 
							job.getConfiguration());
					}
					else {
						FileStatus[] status = fs.listStatus(new Path(rank_output.toString() + "/" + TwitterRankImpl2.ITERATION + (iter-1) ));
						
						for (FileStatus stat : status){
							DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
								job.getConfiguration());
						}
					}
					
			
					String hdfs_dir = rank_output.toString() + "/" + TwitterRankImpl2.ITERATION + iter;
					Path outputDir = new Path(hdfs_dir);	
					
					if(fs.exists(outputDir))
						fs.delete(outputDir, true);
					
					FileInputFormat.setInputPaths(job, transit_matrix);
					
					FileOutputFormat.setOutputPath(job, outputDir);
					
					job.setJarByClass(TwitterRankImpl2.class);
					job.setJobName("Twitter rank for " + rank_output.getName() + " iteration " + iter);
					job.setMapperClass(TwitterRankImpl2Mapper.class);
					job.setMapOutputKeyClass(LongWritable.class);
					job.setMapOutputValueClass(Text.class);
					
					//job.setCombinerClass(TwitterRankImpl2Reducer.class);
					job.setReducerClass(TwitterRankImpl2Reducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(FloatWritable.class);
					
					job.setNumReduceTasks(1);
						
					if(job.waitForCompletion(true)){
						Path last_iteration = new Path(rank_output.toString() + "/" + TwitterRankImpl2.ITERATION + (iter-1));
						fs.delete(last_iteration, true);
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
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
				
			}
			return true;
		}
	}
	
	static class TwitterRankImpl2Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {						

			
			String [] fields = value.toString().split("\\|");
			
			
			// The transit matrix has followers as row and friends as column
			String itemSK = fields[0];
			Long followerID = Long.parseLong(fields[1]);
			
			context.write(new LongWritable(followerID), new Text(fields[2] + "|" + fields[3] + "|" + itemSK));

		}
	}
	
	static class TwitterRankImpl2Reducer extends Reducer<LongWritable, Text, Text, FloatWritable> {
		
		// map for the user rankings. (user:rank).
		private Map<String, Float> map_ranks = new HashMap<String, Float>();
		
		// map for the random suffer vector. (user:random jump probability).
		private Map<String, Float> map_RSV = new HashMap<String, Float>();
		
		// keep track of the iteration the algorithm has run.
		private Integer iteration;
		
		@Override
		protected void setup(final Context context) throws IOException {

			Configuration mapred_config = context.getConfiguration();
			iteration = mapred_config.getInt(TwitterRankImpl2.ITERATION, 0);
			
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
							map_ranks.put(userID, Float.parseFloat(rank));
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
							map_RSV.put(userID, Float.parseFloat(jump_prob));
						}
						line = in.readLine();
					}
					in.close();
				
				}
				else if (p.toString().contains("part") && p.toString().contains(TwitterRankImpl2.ITERATION)) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					while (line != null) {
						String [] fields = line.split("\\|");
			
						String userID = fields[1];
						String rank = fields[2];
			
						if (!userID.equals("") && !rank.equals("")) {
							map_ranks.put(userID, Float.parseFloat(rank));
						}
						line = in.readLine();
					}
					in.close();
				}
			}
		}
		
		/**
		 * The reduce output is with this format:
		 * 
		 * itemSK|userID|influence score
		 */
		@Override
	    protected void reduce(LongWritable key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			float new_rank = 0;
			float alpha = 0.85f;
			
			String itemSK = null;
			for (Text value : values) {
				String [] fields = value.toString().split("\\|");
				
				String friend = fields[0];
				Float transit_prob = Float.parseFloat(fields[1]);
				itemSK = fields[2];
				
				new_rank += alpha * transit_prob * map_ranks.get(friend) + (1 - alpha) * map_RSV.get(friend);
				
			}
			
			assert(itemSK != null);
			
			context.write(new Text(itemSK + "|" + key.toString()), new FloatWritable(new_rank));
	    }
	}


	
	@Override
	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			final FileSystem fs = FileSystem.get(mapred_config);
				
			PathFilter clusterFileFilter = new PathFilter() {
				  @Override
				  public boolean accept(Path path) {
				    return path.getName().startsWith("part");
				  }
			};
			
			FileStatus[] initial_rank_files = fs.listStatus(new Path(TwitterRankConstant.TWITTER_RANK()
					+"/"+TwitterRankConstant.INITIAL_RANK()), clusterFileFilter);
			
			FileStatus[] rand_suffer_files = fs.listStatus(new Path(TwitterRankConstant.TWITTER_RANK()
					+"/"+TwitterRankConstant.RAND_SUFFER_VECTOR()), clusterFileFilter);
			
			FileStatus[] transit_matrix_files = fs.listStatus(new Path(TwitterRankConstant.TWITTER_RANK()
					+"/"+TwitterRankConstant.TRANSIT_MATRIX()), clusterFileFilter);
			//Set<String> promoted_prod = new HashSet<String>();
			
			assert(initial_rank_files.length == rand_suffer_files.length &&
					 rand_suffer_files.length == transit_matrix_files.length);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
			
			// Run TwitterRank algorithm for each product.
			
			ExecutorService pool = Executors.newFixedThreadPool(4);
			
			List<Future<Boolean>> future_tasks = new LinkedList<Future<Boolean>>();
			for (int i = 0; i < initial_rank_files.length; i++){
				Path intial_rank = initial_rank_files[i].getPath();
				Path rand_suffer_vec = rand_suffer_files[i].getPath();
				Path transit_matrix = transit_matrix_files[i].getPath();
				Path rank_output = new Path(TwitterRankConstant.TWITTER_RANK() + "/" + "product"+i);
			
								
				SingleTwitterRank tr = new SingleTwitterRank(mapred_config, fs, intial_rank, rand_suffer_vec, 
						transit_matrix, rank_output);
				
				Future<Boolean> future = pool.submit(tr);
				future_tasks.add(future);
									
			}	
			
			Boolean all_succeed = true;
			
			/**
			 * Check if all sub-jobs are successfully.
			 */
			for(Future<Boolean> task : future_tasks) {
				if(task.get()) {
					continue;
				}
				else {
					all_succeed = false;
					break;
				}
			}
			
			pool.shutdown();
			
			if(all_succeed)
				return true;
			else 
				return false;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//		} catch (InterruptedException e) {
//	         System.out.print(e);
	    } catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
}
