package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import bigframe.workflows.util.TextTextPair;

public class RankAndSuffecVecVertexInputFormat<E extends Writable, M extends Writable>
    extends TextVertexValueInputFormat<Text, Text, E, M> {

  @Override
  public TextVertexValueReader createVertexValueReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new RankAndSuffVecVertexValueReader();
  }

  public class RankAndSuffVecVertexValueReader extends
      TextVertexValueReaderFromEachLineProcessed<TextTextPair> {

    @Override
    protected TextTextPair preprocessLine(Text line) throws IOException {
      String[] fields = line.toString().split("\001");

      return new TextTextPair(new Text(fields[0] + "|" + fields[1]), new Text(
          fields[2] + "|" + fields[3]));
    }

    @Override
    protected Text getId(TextTextPair data) throws IOException {
      return data.getFirst();
    }

    @Override
    protected Text getValue(TextTextPair data) throws IOException {
      return data.getSecond();
    }
  }

}
