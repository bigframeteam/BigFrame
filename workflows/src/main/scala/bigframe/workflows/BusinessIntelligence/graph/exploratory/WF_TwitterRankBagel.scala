package bigframe.workflows.BusinessIntelligence.graph.exploratory

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

import bigframe.workflows.runnable.BagelRunnable
import bigframe.workflows.Query

import bigframe.workflows.util.TREdge
import bigframe.workflows.util.TRVertex
import bigframe.workflows.util.TRMessage

class WF_TwitterRankBagel(rankandsuffvec_dir: String, transitmatrix_dir: String, 
		numItr: Int, alpha: Double, numPartition: Int, output_dir: String) 
		extends Query with BagelRunnable with Serializable {

	def runBagel(sc: SparkContext): Boolean = { 
		
		val rankandsuffvec = sc.textFile(rankandsuffvec_dir)
								.map( line => {
										val t = line.split("\001")
										(t(0) + "|" + t(1), (t(2).toDouble, t(3).toDouble))
								})

		// This is exactly following the TwitterRank Paper. 
		// There should be something wrong about the algorithm, but for benchmark, the speed would be similar.
		val transitmatrix = sc.textFile(transitmatrix_dir)
								.map( line => {
										val t = line.split("\001")
										(t(0) + "|" + t(2), (t(0) + "|" + t(1), t(3).toDouble))
								}).groupByKey()
								
		
		val verts = rankandsuffvec.join(transitmatrix).map( t => {
										val links = t._2._2.map( k => new TREdge(k._1, k._2)).toArray
										(t._1, new TRVertex(t._1, t._2._1._1, t._2._1._2, links, true))
								}).cache
		
		val emptyMsgs = sc.parallelize(Array[(String, TRMessage)]())

		// The computation stub for TwitterRank
		def compute(self: TRVertex, msgs: Option[Array[TRMessage]], superstep: Int)
			: (TRVertex, Array[TRMessage]) = {
			val msgSum = msgs.getOrElse(Array[TRMessage]()).map(_.rankShare).sum
			
			val newRank = alpha * msgSum + (1-alpha) * self.teleport
					
			val halt = superstep >= numItr
			
			val msgsOut = 
				if(!halt)
					self.outEdges.map(edge =>
						new TRMessage(edge.targetID, newRank * edge.transitProb)).toArray
				else
					Array[TRMessage]()
			
			(new TRVertex(self.id, newRank, self.teleport ,self.outEdges, !halt), msgsOut)
		}
		
		val result = Bagel.run(sc, verts, emptyMsgs, numPartition)(compute)
	
		result.map(_._2).saveAsTextFile(output_dir)
		
		true
	}

	def printDescription(): Unit = {}

}