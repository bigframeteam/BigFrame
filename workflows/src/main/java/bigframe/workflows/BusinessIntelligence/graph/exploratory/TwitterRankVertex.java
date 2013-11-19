package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TwitterRankVertex extends Vertex<Text, Text, DoubleWritable, DoubleWritable> {

	@Override
	public void compute(Iterable<DoubleWritable> messages) throws IOException {
	    if (getSuperstep() > 0) {
	    	double twitterRank = 0.0;
	    	for (DoubleWritable message : messages) {
	    		twitterRank += message.get();
	    	}
	   
    		System.out.println("supperstep:" + getSuperstep());
    		String current_value = getValue().toString();
    		System.out.println("Vertex id:" + getId());
    		System.out.println("Vertex value:" + getValue());
    		String transit_prob = current_value.split("\\|")[1];
    		twitterRank = 0.85 * twitterRank + 0.15 * Double.parseDouble(transit_prob);
    		setValue(new Text(twitterRank + "|" + transit_prob));

	    }

	    if (getSuperstep() < 10) {
	    	sendMessageToAllEdges(new 
	    			DoubleWritable(Double.parseDouble(getValue().toString().split("\\|")[0])));
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
