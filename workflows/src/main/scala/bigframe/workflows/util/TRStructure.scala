package bigframe.workflows.util

import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._


class TREdge (val targetID: String, val transitProb: Double) extends Serializable

class TRVertex(
		val id: String, val rank: Double, val teleport: Double, val outEdges: Array[TREdge],
		val active: Boolean) extends Vertex with Serializable {
	
	override def toString() = id + "|" + rank
	
}
 
class TRMessage(
		val targetId: String, val rankShare: Double) extends Message[String] with Serializable