package bigframe.workflows.BusinessIntelligence.graph.exploratory

import bigframe.workflows.runnable.GiraphRunnable
import bigframe.workflows.Query

import org.apache.giraph.job.GiraphJob
import org.apache.giraph.conf.GiraphClasses
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat
import org.apache.giraph.hive.input.edge.HiveToEdge
import org.apache.giraph.hive.input.vertex.HiveToVertex
import org.apache.giraph.hive.input.vertex.HiveVertexInputFormat
import org.apache.giraph.hive.output.HiveVertexOutputFormat
import org.apache.giraph.hive.output.HiveVertexWriter
import org.apache.giraph.hive.output.VertexToHive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

import org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_SPLITS
import org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_EDGE_CLASS
import org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_VERTEX_CLASS
import org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_SPLITS
import org.apache.giraph.hive.common.HiveProfiles.EDGE_INPUT_PROFILE_ID
import org.apache.giraph.hive.common.HiveProfiles.VERTEX_INPUT_PROFILE_ID
import org.apache.giraph.hive.common.HiveProfiles.VERTEX_OUTPUT_PROFILE_ID

import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.input.HiveInputDescription;
import com.facebook.giraph.hive.output.HiveApiOutputFormat;
import com.facebook.giraph.hive.output.HiveOutputDescription;
import com.facebook.giraph.hive.schema.HiveTableSchemas;

/**
 * Just for proof of concept.
 * 
 *  @author andy
 */
class WF_TwitterRank(transitMatrix: String, initialRank: String, randSuffVec: String) extends Query with GiraphRunnable{

	def printDescription(): Unit = {}
	
	override def runGiraph(mapred_config: Configuration): Boolean = {
		
		HIVE_TO_VERTEX_CLASS.set(mapred_config, classOf[InitialRankToVertex]);
		mapred_config.setClass(HiveVertexWriter.VERTEX_TO_HIVE_KEY, 
				classOf[TRVertexToHive], classOf[VertexToHive[Text, DoubleWritable, Writable]]);
		
		val job = new GiraphJob(mapred_config, getClass().getName())
		var giraphConf = job.getConfiguration()
		
		
		var hiveVertexInputDescription = new HiveInputDescription();
		var hiveEdgeInputDescription = new HiveInputDescription();
		var hiveOutputDescription = new HiveOutputDescription();
		

		
		hiveVertexInputDescription.setNumSplits(HIVE_VERTEX_SPLITS.get(giraphConf));
		HiveApiInputFormat.setProfileInputDesc(mapred_config, hiveVertexInputDescription,
				VERTEX_INPUT_PROFILE_ID);
		giraphConf.setVertexInputFormatClass(classOf[HiveVertexInputFormat[Text, DoubleWritable, Writable]]);
		HiveTableSchemas.put(giraphConf, VERTEX_INPUT_PROFILE_ID,
				hiveVertexInputDescription.hiveTableName());
		
		return true
	}

}