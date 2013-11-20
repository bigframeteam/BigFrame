package bigframe.workflows.BusinessIntelligence.graph.exploratory

import bigframe.workflows.runnable.GiraphRunnable
import bigframe.workflows.Query

import org.apache.giraph.job.GiraphJob
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.io.formats.GiraphFileInputFormat

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable

/**
 * A Giraph implementation of the TwitterRank algorithm.
 * 
 *  @authou andy 
 */
class WF_TwitterRankGiraph extends Query with GiraphRunnable {

	def printDescription(): Unit = {}

	override def runGiraph( giraph_config: GiraphConfiguration): Boolean = {
		
		val workers = 2
		
		var giraph_config_copy = new GiraphConfiguration(giraph_config)
		var job = new GiraphJob(giraph_config_copy, getClass.getName);	
		var giraphConfiguration = job.getConfiguration()
		
		/**
		 * Initialize vertex input
		 */
		GiraphFileInputFormat.addVertexInputPath(giraphConfiguration, new Path("/user/hive/warehouse/rankandsuffervec"))
		giraphConfiguration.setVertexInputFormatClass(
				classOf[RankAndSuffecVecVertexInputFormat[NullWritable, NullWritable]])
		
		/**
		 *  Initialize edge input
		 */		
		GiraphFileInputFormat.addEdgeInputPath(giraphConfiguration,  new Path("/user/hive/warehouse/transitmatrix"))		
		giraphConfiguration.setEdgeInputFormatClass(
				classOf[TransitMatrixEdgeInputFormat])
		
		/**
		 * Initialize vertex output
		 */
		val fs = FileSystem.get(giraphConfiguration)
		val output_path = new Path("twitterrank")
		
		if(fs.exists(output_path))
			fs.delete(output_path, true)
		
		FileOutputFormat.setOutputPath(job.getInternalJob(), output_path);
		giraphConfiguration.setVertexOutputFormatClass(classOf[TwitterRankVertexOutputFormat])
				
		/**
		 * Set computation class
		 */
		giraphConfiguration.setVertexClass(classOf[TwitterRankVertex])
		giraphConfiguration.setWorkerConfiguration(workers, workers, 100.0f)
		
		return if(job.run(true)) true else false 
	}
}