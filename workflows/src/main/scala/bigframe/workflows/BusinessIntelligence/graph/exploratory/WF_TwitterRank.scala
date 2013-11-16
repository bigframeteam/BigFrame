package bigframe.workflows.BusinessIntelligence.graph.exploratory

import bigframe.workflows.BusinessIntelligence.RTG.exploratory.TwitterRankConstant
import bigframe.workflows.runnable.HiveGiraphRunnable
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

import com.facebook.giraph.hive.input.HiveApiInputFormat
import com.facebook.giraph.hive.input.HiveInputDescription
import com.facebook.giraph.hive.output.HiveApiOutputFormat
import com.facebook.giraph.hive.output.HiveOutputDescription
import com.facebook.giraph.hive.schema.HiveTableSchemas

import java.sql.Connection

/**
 * Just for proof of concept.
 * 
 *  @author andy
 */
class WF_TwitterRank() extends Query with HiveGiraphRunnable{

	def printDescription(): Unit = {}
	
	override def prepareHiveGiraphTables(connection: Connection): Unit = {
  		val stmt = connection.createStatement()
  		
  		stmt.execute("Drop Table twitterRank")
  		
  		val create_twitterRank = "Create TABLE twitterRank" +
  				"	(" +
  				"		item_sk		int," +
  				"		user_id		int," +
  				"		rank_score	float" +
  				"	)"
  		
  		stmt.execute(create_twitterRank)
	}
	
	override def runGiraph(hive_config: HiveConf): Boolean = {
		
	  /**
	   * Get the copy of the hive configuration
	   */
		val hive_config_copy = new HiveConf(hive_config)
	  
		val workers = 1	
		val dbName = "default"
		val edgeInputTableStr = "initialRank"
		val vertexInputTableStr = "transitMatrix"
		val vertexOutputTableStr = "twitterRank"
	  
		HIVE_TO_VERTEX_CLASS.set(hive_config_copy, classOf[InitialRankToVertex])
		hive_config_copy.setClass(HiveVertexWriter.VERTEX_TO_HIVE_KEY, 
				classOf[TRVertexToHive], classOf[VertexToHive[Text, DoubleWritable, Writable]])
		
		val job = new GiraphJob(hive_config_copy, getClass().getName())
		var giraphConf = job.getConfiguration()
		
		
		var hiveVertexInputDescription = new HiveInputDescription()
		var hiveEdgeInputDescription = new HiveInputDescription()
		var hiveOutputDescription = new HiveOutputDescription()
		
		/**
		 * Initialize hive input db and tables
		 */
		hiveVertexInputDescription.setDbName(dbName)
		hiveEdgeInputDescription.setDbName(dbName)
		hiveOutputDescription.setDbName(dbName)
		
		
		hiveEdgeInputDescription.setTableName(edgeInputTableStr)
		hiveVertexInputDescription.setTableName(vertexInputTableStr)
		hiveOutputDescription.setTableName(vertexOutputTableStr)

		/**
		 * Initialize the hive input settings
		 */
		hiveVertexInputDescription.setNumSplits(HIVE_VERTEX_SPLITS.get(giraphConf))
		HiveApiInputFormat.setProfileInputDesc(giraphConf, hiveVertexInputDescription,
				VERTEX_INPUT_PROFILE_ID)
		giraphConf.setVertexInputFormatClass(classOf[HiveVertexInputFormat[Text, DoubleWritable, Writable]])
		HiveTableSchemas.put(giraphConf, VERTEX_INPUT_PROFILE_ID,
				hiveVertexInputDescription.hiveTableName())
		
		hiveEdgeInputDescription.setNumSplits(HIVE_EDGE_SPLITS.get(giraphConf));
		HiveApiInputFormat.setProfileInputDesc(giraphConf, hiveEdgeInputDescription,
				EDGE_INPUT_PROFILE_ID);
		giraphConf.setEdgeInputFormatClass(classOf[HiveEdgeInputFormat[Text, DoubleWritable]]);
		HiveTableSchemas.put(giraphConf, EDGE_INPUT_PROFILE_ID,
				hiveEdgeInputDescription.hiveTableName())		
		
				
		/**
		 * Initialize the hive output settings
		 */
		HiveApiOutputFormat.initProfile(giraphConf, hiveOutputDescription,
				VERTEX_OUTPUT_PROFILE_ID);
		giraphConf.setVertexOutputFormatClass(classOf[HiveVertexOutputFormat[Text, DoubleWritable, Writable]]);
		HiveTableSchemas.put(giraphConf, VERTEX_OUTPUT_PROFILE_ID,
				hiveOutputDescription.hiveTableName());	
		
		/**
		 * Set number of workers
		 */
		giraphConf.setWorkerConfiguration(workers, workers, 100.0f)	
		
		/**
		 * Run the job
		 */
		if (job.run(true)) return true else return false
	}

}