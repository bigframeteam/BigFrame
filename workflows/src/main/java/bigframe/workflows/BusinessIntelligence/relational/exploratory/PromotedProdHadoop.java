package bigframe.workflows.BusinessIntelligence.relational.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;

/**
 * A class to get the names of the set of promoted products.
 * 
 * @author andy
 *
 */
public class PromotedProdHadoop extends HadoopJob {

	private String item_path;
	private String promotion_tbl;
	private Set<Integer> promotionSKs;
	
	private static String PROMOTION_LIST = "promotion_list";
	
	public PromotedProdHadoop(String rel_path, Set<Integer> promotionSKs, Configuration mapred_config) {
		super(mapred_config);
		item_path = rel_path + "/item";
		promotion_tbl = rel_path + "/promotion";
		this.promotionSKs = promotionSKs;
		
	}

	
	static class PromotedProdMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private List<String> promotIDs = new ArrayList<String>();
		private List<Long> itemSKs = new ArrayList<Long>();
		private List<Integer> dateBeginSKs = new ArrayList<Integer>();
		private List<Integer> dateEndSKs = new ArrayList<Integer>();
		private Set<String> promotedSKs = new HashSet<String>();
		
		@Override
		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();			
			
			Path[] uris = DistributedCache.getLocalCacheFiles(mapred_config);
		  	
			String promoted_list = mapred_config.get(PROMOTION_LIST);
			
			for(String promotSK : promoted_list.split("\\|", -1)) {
				promotedSKs.add(promotSK);
			}
			
	  	   	for(Path p : uris) {	  		   
	  	   		if (p.toString().contains("promotion")) {
		  	   		BufferedReader in = new BufferedReader(new FileReader(p.toString()));
	  	   			String line = in.readLine();
	  	   			while (line != null) {
	  	   				String[] fields = line.split("\\|", -1);

	  	   				String promtSK = fields[0];
	  	   				
	  	   				if(promotedSKs.contains(promtSK)) {
				
		    				String promtID = fields[1]; 
		    			    String datebegSK = fields[2]; 
		    				String dateendSK = fields[3]; 
		    				String itemSK = fields[4];
	
		    				if (!datebegSK.equals("") && !dateendSK.equals("")
		    						&& !itemSK.equals("") && !promtID.equals("")) {
		    					promotIDs.add(promtID);
		    					itemSKs.add(Long.parseLong(itemSK));
		    					dateBeginSKs.add(Integer.parseInt(datebegSK));
		    					dateEndSKs.add(Integer.parseInt(dateendSK));
		  					   
		    				}
	  	   				}
	    				line = in.readLine();
	  	   			}
	  	   			in.close();
	  	   		}	  	   		
	  	   	}
		}
		
		/**
		 * The map output is with this format:
		 * 
		 * itemSK|product name|dateBeginSK|dateEndSK
		 */
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {
			
			String [] fields = value.toString().split("\\|", -1);
			
			long itemSK = 0l;
			String productname = "";
			if (!fields[0].equals("") && !fields[21].equals("")) {
				itemSK = Long.parseLong(fields[0]);
				productname = fields[21];
			}
			
			for (int i = 0; i < itemSKs.size(); i++) {
				if(itemSKs.get(i)== itemSK) {
					context.write(new LongWritable(itemSK), new Text(productname + "|" 
								+ dateBeginSKs.get(i) + "|" + dateEndSKs.get(i)));
				}
			} 				

		}
	}
	
	
//	/**
//	 * A reducer to eliminate duplicate itemSK.
//	 */
//	static class PromotedProdReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
//		
//		
//		/**
//		 * The reduce output is with this format:
//		 * 
//		 *  itemSK|product name
//		 */
//		@Override
//	    protected void reduce(LongWritable key, Iterable<Text> values, 
//	    		final Context context) throws IOException, InterruptedException {
//	    	
//			for(Text value : values) {
//				context.write(key, value);
//				break;
//			}
//	    }
//	}
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);
		
			FileStatus[] status = fs.listStatus(new Path(promotion_tbl));
			int count_promtSK = 0;
			
			// Count the number of promotion SKs.
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
						mapred_config);
				BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));
				
				String line = br.readLine();
				while(line != null) {
					count_promtSK++;						
					line = br.readLine();						
				}
				br.close();
			}	
			
			
			// Guarantee all selected promotionSK exist. 
			String promoted_list = "";
			for(Integer promotionSK : promotionSKs) {
				if(promotionSK > count_promtSK) {
					System.out.println(promotionSK + " does not exist in the promotion table");
					return false;
				}
				promoted_list += promotionSK + "|";
			}
			
			mapred_config.set(PROMOTION_LIST, promoted_list);
			
			String hdfs_dir = ReportSalesConstant.PROMOTED_PROD_PATH();
			Path outputDir = new Path(hdfs_dir);	
	

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			FileInputFormat.setInputPaths(job, new Path(item_path));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJobName("Get promoted Products");
			job.setJarByClass(PromotedProdHadoop.class);
			
			job.setMapperClass(PromotedProdMapper.class);
//			job.setReducerClass(PromotedProdReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(0);

			
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
