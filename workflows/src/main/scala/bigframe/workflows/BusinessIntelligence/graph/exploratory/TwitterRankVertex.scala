package bigframe.workflows.BusinessIntelligence.graph.exploratory

import org.apache.giraph.graph.Vertex

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

class TwitterRankVertex extends Vertex[Text, DoubleWritable, DoubleWritable, DoubleWritable] {

	override def compute(messages: java.lang.Iterable[DoubleWritable]): Unit = {
	    if (getSuperstep() > 0) {
	    	var twitterRank = 0.0
	    	val messages_iter = messages.iterator
	    	while (messages_iter.hasNext) {
	    		twitterRank += messages_iter.next.get
	    	}
	    	setValue(new DoubleWritable(twitterRank))
	    }

	    if (getSuperstep() < 10) {
	    	sendMessageToAllEdges(new DoubleWritable(getValue.get))
	    } else {
	    	voteToHalt()
	    }
    
	}
	
	override def sendMessageToAllEdges(message: DoubleWritable): Unit = {
		getEdges.iterator.foreach(e => sendMessage(e.getTargetVertexId, 
		    new DoubleWritable(message.get * 
		        getEdgeValue(e.getTargetVertexId).get)))
	}

}