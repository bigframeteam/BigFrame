package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class TwitterRankVertexOutputFormat extends
    TextVertexOutputFormat<Text, DoubleWritable, DoubleWritable> {
	
	/**
	 * Simple text based vertex writer
  	*/
	private class TwitterRankVertexWriter extends TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(
				Vertex<Text, DoubleWritable, DoubleWritable, ?> vertex) throws IOException {
			// TODO Auto-generated method stub
			Text line = new Text(vertex.getId().toString() + "|" + vertex.getValue().toString());
			
			return line;
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
	    return new TwitterRankVertexWriter();
	}
}

