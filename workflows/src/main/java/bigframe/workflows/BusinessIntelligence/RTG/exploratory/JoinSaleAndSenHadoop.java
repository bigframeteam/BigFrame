package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigframe.workflows.HadoopJob;
import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant;
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesConstant;

/**
 * Join the sales report and sentiment report by product.
 * 
 * @author andy
 *
 */
public class JoinSaleAndSenHadoop extends HadoopJob {

	public JoinSaleAndSenHadoop(Configuration _mapred_config) {
		super(_mapred_config);
		// TODO Auto-generated constructor stub
	}

	static class JoinSaleAndSenMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
		private Map<String, Float> map_item_sentiment = new HashMap<String, Float>();
		
		@Override
		protected void setup(final Context context) throws IOException {
			Configuration mapred_config = context.getConfiguration();
			Path [] uris = DistributedCache.getLocalCacheFiles(mapred_config);
	  	   
	  	   	for(Path p : uris) {  		   
	  	   		if (p.toString().contains("part")) {
	  	   			BufferedReader in = new BufferedReader(new FileReader(p.toString()));
	  	   			String line = in.readLine();
	  	   			while (line != null) {
	  	   				String [] fields = line.split("\\|");

	    				String item = fields[0];
	    				String sentiment = fields[1];

	    				if (!item.equals("") && !sentiment.equals("")) {
	    					map_item_sentiment.put(item, Float.parseFloat(sentiment));
	    				}
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
			
			String [] fields = value.toString().split("\\|");
			
			String promotID = fields[0];
			String itemSK = fields[1];
			Float sales = Float.parseFloat(fields[2]);
			
			assert(map_item_sentiment.containsKey(itemSK));
			
			context.write(new Text(promotID + "|" + itemSK),
						new FloatWritable(sales * map_item_sentiment.get(itemSK)));
		}
	}
	
	@Override
	public Boolean runHadoop(Configuration mapred_config) {
		// TODO Auto-generated method stub
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		try {
			FileSystem fs = FileSystem.get(mapred_config);

			FileStatus[] status = fs.listStatus(new Path(TwitterRankConstant.GROUP_SEN_BY_PROD()));
			
			for (FileStatus stat : status){
				DistributedCache.addCacheFile(new URI(stat.getPath().toString()), 
					mapred_config);
			}
			
			String hdfs_dir = TwitterRankConstant.REPORT_SALES_SEN();
			Path outputDir = new Path(hdfs_dir);	
		

			if(fs.exists(outputDir))
				fs.delete(outputDir, true);
			
			// For hadoop 1.0.4
			mapred_config.set("mapred.textoutputformat.separator", "|");	
		
			Job job;
		
			job = new Job(mapred_config);
			
			FileInputFormat.addInputPath(job, new Path(ReportSalesConstant.REPORT_SALES()));
			
			FileOutputFormat.setOutputPath(job, outputDir);
			
			job.setJarByClass(JoinSaleAndSenHadoop.class);
			job.setJobName("Join sales and sentiment score by product");
			

			job.setMapperClass(JoinSaleAndSenMapper.class);
			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(FloatWritable.class);
			

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
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
