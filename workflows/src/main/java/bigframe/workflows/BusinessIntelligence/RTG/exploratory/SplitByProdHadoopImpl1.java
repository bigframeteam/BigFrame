package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;

public class SplitByProdHadoopImpl1 extends HadoopJob {

	public SplitByProdHadoopImpl1(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}


	private String promotion_tbl = ReportSalesConstant.PROMOTED_PROD_PATH();
	private static String FILTER_BY_ITEMSK = "filter.by.itemSK";
	private static String OUTPUT_PREFIX = "output.prefix";
	

	static class  SplitByProdMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String filter_by_itemSK;
		//private MultipleOutputs<Text, NullWritable> mos;
		private String prefix;
		

		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();
			filter_by_itemSK = mapred_config.get(SplitByProdHadoopImpl1.FILTER_BY_ITEMSK);
			prefix = mapred_config.get(SplitByProdHadoopImpl1.OUTPUT_PREFIX);
			
			//mos = new MultipleOutputs<Text, NullWritable>(context);
		}
		
		/**
		 * Only the lines with the selected products are output.
		 *  
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			

			String itemSK = value.toString().split("\\|")[0];

			if(itemSK.equals(filter_by_itemSK)) {
				//mos.write(prefix, value, null);
				context.write(value, null);
			}
			
		}
		
//		@Override
//		protected void cleanup(final Context context) throws IOException, InterruptedException {
//			 mos.close();
//		}
	}
	
	private void getPromotedProducts(BufferedReader br, Set<String> promoted_prod) throws IOException {
        String line=br.readLine();
        while (line != null){
        	String itemSK = line.split("\\|")[0];
        	promoted_prod.add(itemSK);  
        	line=br.readLine();
        }
	}
	
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			/**
			 * Create the parent directory if it is not exist
			 */
			String parent_dir = TwitterRankConstant.TWITTER_RANK();
			Path parentPath = new Path(parent_dir);
			if(!fs.exists(parentPath))
				fs.mkdirs(parentPath);
				
			FileStatus[] status = fs.listStatus(new Path(promotion_tbl));
			Set<String> promoted_prod = new HashSet<String>();
			
			for (FileStatus stat : status){
				if(stat.getPath().toString().contains("part")) {
					BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));
								
					getPromotedProducts(br, promoted_prod); 
				}
			}			
			
			// For hadoop 1.0.4			
			//mapred_config.set("mapred.textoutputformat.separator", "|");
			
			for(String prod : promoted_prod) {
				
				mapred_config.set(SplitByProdHadoopImpl1.FILTER_BY_ITEMSK, prod);			
			
				/**
				 * Create the parent directory if it is not exist
				 */
				String prod_dir = parent_dir + "/" +prod;
				Path prodPath = new Path(prod_dir);
				if(!fs.exists(prodPath))
					fs.mkdirs(prodPath);
				
				/**
				 * Filter the initial_rank by product name.
				 */
				
				String hdfs_dir1 = prod_dir + "/" + TwitterRankConstant.INITIAL_RANK();
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
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(NullWritable.class);
				
				job1.setNumReduceTasks(0);
				
				job1.submit();
			
				/**
				 * Filter the random suffer vector by product name.
				 */
				String hdfs_dir2 = prod_dir + "/" + TwitterRankConstant.RAND_SUFFER_VECTOR();
				Path outputDir2 = new Path(hdfs_dir2);	

				if(fs.exists(outputDir2))
					fs.delete(outputDir2, true);
				
				Job job2;
			
				job2 = new Job(mapred_config);
				FileInputFormat.setInputPaths(job2, new Path(TwitterRankConstant.RAND_SUFFER_VECTOR()));
				
				FileOutputFormat.setOutputPath(job2, outputDir2);
				
//				MultipleOutputs.addNamedOutput( job2, "suffer_vector", TextOutputFormat.class,
//						Text.class, NullWritable.class );
//				mapred_config.set(SplitByProdHadoop.OUTPUT_PREFIX, "suffer_vector");
				
				job2.setJarByClass( SplitByProdHadoopImpl1.class);
				job2.setJobName("split random suffer vector");
				job2.setMapperClass( SplitByProdMapper.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(NullWritable.class);
				
				job2.setNumReduceTasks(0);
				
				job2.submit();
				
				/**
				 * Filter the transit matrix by product name.
				 */
				String hdfs_dir3 = prod_dir + "/" + TwitterRankConstant.TRANSIT_MATRIX();
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
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(NullWritable.class);
				
				job3.setNumReduceTasks(0);
				
				job3.submit();				
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
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
}
