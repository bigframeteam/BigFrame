package bigframe.workflows.BusinessIntelligence.graph.exploratory

import com.facebook.giraph.hive.record.HiveReadableRecord
import org.apache.giraph.hive.input.vertex.SimpleNoEdgesHiveToVertex
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

class RandSuffVecToVertex extends SimpleNoEdgesHiveToVertex[Text, DoubleWritable] {

  override def getVertexId(hiveRecord: HiveReadableRecord): Text = 
	  new Text(hiveRecord.getLong(0) + "|" + hiveRecord.getLong(1))

  override def getVertexValue(hiveRecord: HiveReadableRecord): DoubleWritable = 
	  new DoubleWritable(hiveRecord.getDouble(2))

}