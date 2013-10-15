package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;

public class SplitByProdHadoopImpl2 extends HadoopJob {
	
	private String promotion_tbl = ReportSalesConstant.PROMOTED_PROD_PATH();

	public SplitByProdHadoopImpl2(Configuration mapred_config) {
		super(mapred_config);

	}

	static class  SplitByProdMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private Map<String, Integer> promoted_prod = new HashMap<String, Integer>();
		
		@Override
		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();
			Path[] uris = DistributedCache.getLocalCacheFiles(mapred_config);
		
			   
			for(Path p : uris) {  		   
				if (p.toString().contains("part")) {
					BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					String line = in.readLine();
					
					int size = 0;
					while (line != null) {
			
			        	String itemSK = line.split("\\|")[0];
			        	promoted_prod.put(itemSK, size);

			        	size++;
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

			String itemSK = value.toString().split("\\|")[0];

			context.write(new IntWritable(promoted_prod.get(itemSK)), value);

			
		}
		
	}
	
	/**
	 * Each output contains only one product.
	 * 
	 * @author andy
	 *
	 */
	static class SplitByProdReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	    
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, 
	    		final Context context) throws IOException, InterruptedException {
			
			for(Text value : values) {
				context.write(null, value);
			}
	    }
	}
	
	private void getPromotedProducts(BufferedReader br, Map<String, Integer> promoted_prod) throws IOException {
        String line=br.readLine();
        int size = promoted_prod.size();
        while (line != null){
        	String itemSK = line.split("\\|")[0];
        	if(promoted_prod.containsKey(itemSK)) {
        		System.out.println(itemSK + " is exited");
        	}
        	promoted_prod.put(itemSK, size);
        	
        	size++;
        	line=br.readLine();
        }
        
	}
	
	
	@Override
	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(promotion_tbl));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
			
			/**
			 * Create the parent directory if it is not exist
			 */
			String parent_dir = TwitterRankConstant.TWITTER_RANK();
			Path parentPath = new Path(parent_dir);
			if(!fs.exists(parentPath))
				fs.mkdirs(parentPath);
			else
				fs.delete(parentPath, true);
				
			FileStatus[] status2 = fs.listStatus(new Path(promotion_tbl));
			Map<String, Integer> promoted_prod = new HashMap<String, Integer>();
			
			for (FileStatus stat : status2){
				if(stat.getPath().toString().contains("part")) {
					BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));
								
					getPromotedProducts(br, promoted_prod); 
				}
			}			
			
			// For hadoop 1.0.4			
			//mapred_config.set("mapred.textoutputformat.separator", "|");
						
			
				/**
				 * Create the parent directory if it is not exist
				 */
				
				/**
				 * Filter the initial_rank by product name.
				 */
				
			String hdfs_dir1 =  TwitterRankConstant.TWITTER_RANK() + "/" + TwitterRankConstant.INITIAL_RANK();
			Path outputDir1 = new Path(hdfs_dir1);	

			if(fs.exists(outputDir1))
				fs.delete(outputDir1, true);
			
			Job job1;
		
			job1 = new Job(mapred_config);
			FileInputFormat.setInputPaths(job1, new Path(TwitterRankConstant.INITIAL_RANK()));
			
			FileOutputFormat.setOutputPath(job1, outputDir1);
			
//				MultipleOutputs.addNamedOutput( job1, "rank", TextOutputFormat.class,
//						Text.class, NullWritable.class );
//				mapred_config.set(SplitByProdHadoop.OUTPUT_PREFIX, "rank");
			
			job1.setJarByClass( SplitByProdHadoopImpl1.class);
			job1.setJobName("split inital rank");
			job1.setMapperClass( SplitByProdMapper.class);
			job1.setReducerClass(SplitByProdReducer.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(NullWritable.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setNumReduceTasks(promoted_prod.size());
			
			job1.submit();
		
			/**
			 * Filter the random suffer vector by product name.
			 */
			String hdfs_dir2 =  TwitterRankConstant.TWITTER_RANK() + "/" + TwitterRankConstant.RAND_SUFFER_VECTOR();
			Path outputDir2 = new Path(hdfs_dir2);	

			if(fs.exists(outputDir2))
				fs.delete(outputDir2, true);
			
			Job job2;
		
			job2 = new Job(mapred_config);
			FileInputFormat.setInputPaths(job2, new Path(TwitterRankConstant.RAND_SUFFER_VECTOR()));
			
			FileOutputFormat.setOutputPath(job2, outputDir2);
			
			
			job2.setJarByClass( SplitByProdHadoopImpl1.class);
			job2.setJobName("split random suffer vector");
			job2.setMapperClass( SplitByProdMapper.class);
			job2.setReducerClass(SplitByProdReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setNumReduceTasks(promoted_prod.size());
			
			job2.submit();
			
			/**
			 * Filter the transit matrix by product name.
			 */
			String hdfs_dir3 =  TwitterRankConstant.TWITTER_RANK() + "/" + TwitterRankConstant.TRANSIT_MATRIX();
			Path outputDir3 = new Path(hdfs_dir3);	

			if(fs.exists(outputDir3))
				fs.delete(outputDir3, true);
			
			Job job3;
		
			job3 = new Job(mapred_config);
			FileInputFormat.setInputPaths(job3, new Path(TwitterRankConstant.TRANSIT_MATRIX()));
			
			FileOutputFormat.setOutputPath(job3, outputDir3);
			
			job3.setJarByClass( SplitByProdHadoopImpl1.class);
			job3.setJobName("split transit matrix");
			job3.setMapperClass( SplitByProdMapper.class);
			job3.setReducerClass(SplitByProdReducer.class);
			job3.setMapOutputKeyClass(IntWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(NullWritable.class);
			job3.setOutputValueClass(Text.class);
			
			job3.setNumReduceTasks(promoted_prod.size());
			
			job3.submit();				
					
			while(!(job1.isComplete()&&job2.isComplete()&&job3.isComplete())) {								
				System.out.println("Spliting Initial Ranking Job Progress:" + "map: " 
						+ (int)(job1.mapProgress() * 100) + "%" + "\t" + "reduce: " + (int)(job1.reduceProgress() * 100) + "%");
				System.out.println("Spliting Random Suffer Vector Job Progress:" + "map: " 
						+ (int)(job2.mapProgress() * 100) + "%" + "\t" + "reduce: " + (int)(job2.reduceProgress() * 100) + "%");
				System.out.println("Spliting Transit Matrix Job Progress:" + "map: " 
						+ (int)(job3.mapProgress() * 100) + "%" + "\t" + "reduce: " + (int)(job3.reduceProgress() * 100) + "%");
				
				Thread.sleep(3000);
			}
			
			if(job1.isSuccessful() && job2.isSuccessful() && job3.isSuccessful()) {
				return true;
			}
			
			else
				return false;
			
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
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

}
