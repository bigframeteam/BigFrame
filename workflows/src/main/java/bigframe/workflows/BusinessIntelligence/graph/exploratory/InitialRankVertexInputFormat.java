package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import bigframe.workflows.util.TextDoublePair;


public class InitialRankVertexInputFormat <E extends Writable,
	M extends Writable> extends TextVertexValueInputFormat<Text, DoubleWritable, E, M>{
	

	@Override
	public TextVertexValueReader createVertexValueReader(
	      InputSplit split, TaskAttemptContext context) throws IOException {
	    return new InitialRankVertexValueReader();
	}
	
	public class InitialRankVertexValueReader extends
		TextVertexValueReaderFromEachLineProcessed<TextDoublePair> {

	    @Override
	    protected TextDoublePair preprocessLine(Text line) throws IOException {
	    	String [] fields = line.toString().split("\001");
	   
	    	
			return new TextDoublePair(new Text(fields[0] + "|" + fields[1]), 
					new DoubleWritable(Double.parseDouble(fields[2])));
	    }
	
	    @Override
	    protected Text getId(TextDoublePair data) throws IOException {
	    	return data.getFirst();
	    }
	
	    @Override
	    protected DoubleWritable getValue(TextDoublePair data) throws IOException {
	    	return data.getSecond();
	    }
  }



}
