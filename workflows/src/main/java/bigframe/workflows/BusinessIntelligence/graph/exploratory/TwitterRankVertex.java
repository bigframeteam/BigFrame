package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TwitterRankVertex extends Vertex<Text, DoubleWritable, DoubleWritable, DoubleWritable> {

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
	    if (getSuperstep() > 0) {
	    	double twitterRank = 0.0;
	    	int size = 0;
	    	for (DoubleWritable message : messages) {
	    		twitterRank += message.get();
	    		size++;
	    	}
	    	if(size > 0)
	    		setValue(new DoubleWritable(twitterRank));
	    }

	    if (getSuperstep() < 10) {
	    	sendMessageToAllEdges(new DoubleWritable(getValue().get()));
	    } else {
	    	voteToHalt();
	    }
		
	}
	
	@Override
	public void sendMessageToAllEdges(DoubleWritable message) {
		
		Iterable<Edge<Text, DoubleWritable>> edges = getEdges();
		
		for(Edge<Text, DoubleWritable> edge : edges) {
			sendMessage(edge.getTargetVertexId(), new DoubleWritable(message.get() * 
			        getEdgeValue(edge.getTargetVertexId()).get()));
		}
	}

}
