package bigframe.workflows.BusinessIntelligence.graph.exploratory

import com.facebook.giraph.hive.record.HiveReadableRecord
import org.apache.giraph.hive.input.edge.SimpleHiveToEdge
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

class TransitMatrixToEdge extends SimpleHiveToEdge[Text, DoubleWritable] {

  override def getSourceVertexId(hiveRecord: HiveReadableRecord): Text = 
	  new Text(hiveRecord.getLong(0) + "|" + hiveRecord.getLong(1))

  override def getTargetVertexId(hiveRecord: HiveReadableRecord): Text = 
	  new Text(hiveRecord.getLong(0) + "|" + hiveRecord.getLong(2))

  override def getEdgeValue(hiveRecord: HiveReadableRecord): DoubleWritable = 
	  new DoubleWritable(hiveRecord.getDouble(3))

}