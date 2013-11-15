package bigframe.workflows.BusinessIntelligence.graph.exploratory

import com.facebook.giraph.hive.record.HiveWritableRecord
import org.apache.giraph.hive.output.SimpleVertexToHive
import org.apache.giraph.graph.Vertex
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

class TRVertexToHive extends SimpleVertexToHive[Text, DoubleWritable, Writable] {

	override def fillRecord(vertex: Vertex[Text, DoubleWritable, Writable, _], 
			record: HiveWritableRecord): Unit = {
		val fields = vertex.getId.toString.split("\\|")
		
		val item_sk = fields(0)
		val user_id = fields(1)
		
		record.set(0, item_sk)
		record.set(1, user_id)
		record.set(2, vertex.getValue().get())
	}

}