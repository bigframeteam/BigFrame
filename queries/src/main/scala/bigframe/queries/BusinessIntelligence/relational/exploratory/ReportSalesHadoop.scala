package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.IOException
import java.net.URI
import java.net.URISyntaxException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


import bigframe.queries.HadoopRunnable

/**
 * Get the daily sales of a set of seleted items.
 * 
 * @author andy
 */
class ReportSalesHadoop(rel_path: String) extends HadoopRunnable {
  
	private val _promotion = rel_path + "/promotion"
	private val _web_sales = rel_path + "/web_sales"
	private val _store_sales = rel_path + "/store_sales"
	private val _catalog_sales = rel_path + "/catalog_sales"
	
	/**
	 * Run the query in Hadoop.
	 */
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	@throws(classOf[ClassNotFoundException])
	override def run(mapred_config: Configuration) = {
	  
		/**
		 * Put the promotion table to distributed cache since it is small enough.
		 */
		try {
			val fs = FileSystem.get(mapred_config)

			val status = fs.listStatus(new Path(_promotion))
			
			
			println("size of status: "+ status.length)
			for (i <- status.indices){
				DistributedCache.addCacheFile(new URI(status(i).getPath().toString()), 
					mapred_config);
			}
		} catch {
			case e: URISyntaxException => e.printStackTrace()
		}
		
		val hdfs_dir = "report_sales"
		val outputDir = new Path(hdfs_dir)	
		
		var job = new Job(mapred_config)
		
		FileInputFormat.addInputPath(job, new Path(_web_sales))
		FileInputFormat.addInputPath(job, new Path(_store_sales))
		FileInputFormat.addInputPath(job, new Path(_catalog_sales))
		
		FileOutputFormat.setOutputPath(job, outputDir)
		
		job.setJarByClass(classOf[ReportSalesHadoop])
		
		job.setMapperClass(classOf[ReportSalesMapper])
		job.setMapOutputKeyClass(classOf[Text])
		job.setMapOutputValueClass(classOf[FloatWritable])
		
		job.setCombinerClass(classOf[ReportSalesReducer])
		
		job.setReducerClass(classOf[ReportSalesReducer])
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[FloatWritable])
		
		job.setNumReduceTasks(1)

		job.waitForCompletion(true)
	}
}