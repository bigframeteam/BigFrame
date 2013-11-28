package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import bigframe.workflows.util.TextTextDoubleRecord;

public class TransitMatrixEdgeInputFormat extends TextEdgeInputFormat<Text, DoubleWritable> {

	@Override
	public EdgeReader<Text, DoubleWritable> createEdgeReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		// TODO Auto-generated method stub
		return new TransitMatrixEdgeReader();
	}
	
	public class TransitMatrixEdgeReader extends
    TextEdgeReaderFromEachLineProcessed<TextTextDoubleRecord> {
		
		@Override
		protected TextTextDoubleRecord preprocessLine(Text line) throws IOException {
			String[] fields = line.toString().split("\001");
			
			return new TextTextDoubleRecord(new Text(fields[0] + "|" + fields[2]), 
					new Text(fields[0] + "|" + fields[1]), new DoubleWritable(Double.parseDouble(fields[3])));
		}

		@Override
		protected Text getSourceVertexId(TextTextDoubleRecord endpoints)
				throws IOException {
			return endpoints.getFirst();
		}

		@Override
		protected Text getTargetVertexId(TextTextDoubleRecord endpoints)
				throws IOException {
			return endpoints.getSecond();
		}

		@Override
		protected DoubleWritable getValue(TextTextDoubleRecord endpoints) throws IOException {
			return endpoints.getValue();
		}
}

}
