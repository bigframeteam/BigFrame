package bigframe.workflows.BusinessIntelligence.relational.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;

/**
 * A class to report the total sales of promoted products within the promotion.
 * 
 * @author andy
 *
 */
public class ReportSalesHadoop extends HadoopJob {
	private String promotion;
	private String web_sales;
	private String store_sales;
	private String catalog_sales;
	private Set<Integer> promotionSKs;
	
	private static String PROMOTION_LIST = "promotion_list";
	
	public ReportSalesHadoop(String rel_path,  Set<Integer> promotionSKs, Configuration mapred_config) {
		super(mapred_config);
		promotion = rel_path + "/promotion";
		web_sales = rel_path + "/web_sales";
		store_sales = rel_path + "/store_sales";
		catalog_sales = rel_path + "/catalog_sales";
		this.promotionSKs = promotionSKs;
	}

	static class ReportSalesMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
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
				BufferedReader in = new BufferedReader(new FileReader(p.toString()));
					   
				if (p.toString().contains("promotion")) {
					String line = in.readLine();
					while (line != null) {
						String[] fields = line.split("\\|", -1);
					
  	   					String promtSK = fields[0];
	  	   				
	  	   				if(promotedSKs.contains(promtSK)) {
							String promtID = fields[1];
							String datebegSK = fields[2];
							String dateendSK = fields[3]; 
							String prodSK = fields[4];
							
							if (!datebegSK.equals("") && !dateendSK.equals("")
									&& !prodSK.equals("") && !promtID.equals("")) {
								promotIDs.add(promtID);
								itemSKs.add(Long.parseLong(prodSK));
								dateBeginSKs.add(Integer.parseInt(datebegSK));
								dateEndSKs.add(Integer.parseInt(dateendSK));
											   
							}
	  	   				}
						line = in.readLine();
					}
				}
				in.close();
			}
		}
		
		@Override
		protected void map(LongWritable key,
				Text value, final Context context)
						throws IOException, InterruptedException {			   
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
				
			String []fields = value.toString().split("\\|", -1);
				
			  
			int sold_dateSK = 0;
			long item_sk = 0L;
			int quantity = 0;
			float price = 0;

			if (filename.contains("store_sales") && 
					!fields[0].equals("") && !fields[2].equals("") && !fields[10].equals("") && !fields[13].equals("")){
				sold_dateSK = Integer.parseInt(fields[0]);
				item_sk = Long.parseLong(fields[2]);
				quantity = Integer.parseInt(fields[10]);
				price = Float.parseFloat(fields[13]);
			}
			else if(filename.contains("web_sales") &&
					!fields[0].equals("") && !fields[3].equals("") && !fields[18].equals("") && !fields[21].equals("")){
				sold_dateSK = Integer.parseInt(fields[0]);
				item_sk = Long.parseLong(fields[3]);
				quantity = Integer.parseInt(fields[18]);
				price = Float.parseFloat(fields[21]);
			}
			else if(filename.contains("catalog_sales") &&
				!fields[0].equals("") && !fields[15].equals("") && !fields[18].equals("") && !fields[21].equals("")){
				sold_dateSK = Integer.parseInt(fields[0]);
				item_sk = Long.parseLong(fields[15]);
				quantity = Integer.parseInt(fields[18]);
				price = Float.parseFloat(fields[21]);
			}


			/**
			 * Group the sales by (promotionID, itemSK)
			 */
			for(int i = 0; i < itemSKs.size(); i++) {
				if(item_sk == itemSKs.get(1) && dateBeginSKs.get(i) <= sold_dateSK && 
					sold_dateSK <= dateEndSKs.get(i)) {
					context.write(new Text(promotIDs.get(i) +"|" +itemSKs.get(i)), new FloatWritable(quantity * price));
				}
			}
										  	  
		}
	}
	
	static class ReportSalesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	    
		/**
		 * The output of reduce is with this format:
		 * 
		 * promotID|itemSK|total sales
		 */
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> values, 
	    		final Context context) throws IOException, InterruptedException {
	    	float sum_sales= 0;
	    	for(FloatWritable value : values) {
	    		sum_sales += value.get();
	    	}
	    	
	    	context.write(key, new FloatWritable(sum_sales));
	    }
	}
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		try {
			FileSystem fs = FileSystem.get(mapred_config);
			
			FileStatus[] status = fs.listStatus(new Path(promotion));
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
	
			String hdfs_dir = ReportSalesConstant.REPORT_SALES();
			Path outputDir = new Path(hdfs_dir);	
	
			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			
			FileInputFormat.addInputPath(job, new Path(web_sales));
			FileInputFormat.addInputPath(job, new Path(store_sales));
			FileInputFormat.addInputPath(job, new Path(catalog_sales));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(ReportSalesHadoop.class);
			job.setJobName("Report sales");
			job.setMapperClass(ReportSalesMapper.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(FloatWritable.class);
			
			job.setCombinerClass(ReportSalesReducer.class);
			job.setReducerClass(ReportSalesReducer.class);
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
