package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.IOException

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
 * Perform a hadoop job for sentiment analyze.
 * 
 * @author andy
 */
class SenAnalyzeHadoop(nested_path: String) extends HadoopRunnable {
	
	private val _tweets_path = nested_path
	
	/**
	 * Run the query in Hadoop.
	 */
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	@throws(classOf[ClassNotFoundException])
	def run(mapred_config: Configuration): Unit = {
		
		val hdfs_dir = "report_sales"
		val outputDir = new Path(hdfs_dir)	
		
		var job = new Job(mapred_config)
		
		FileInputFormat.addInputPath(job, new Path(_tweets_path))
		
		FileOutputFormat.setOutputPath(job, outputDir)
		
		job.setJarByClass(classOf[SenAnalyzeHadoop])
		job.setMapperClass(classOf[SenAnalyzeMapper])
		job.setCombinerClass(classOf[SenAnalyzeReducer])
		job.setReducerClass(classOf[SenAnalyzeReducer])
		
		job.setNumReduceTasks(1)
		
		job.setOutputKeyClass(classOf[Text])
		job.setOutputValueClass(classOf[FloatWritable])

		job.waitForCompletion(true)
		
	}

}