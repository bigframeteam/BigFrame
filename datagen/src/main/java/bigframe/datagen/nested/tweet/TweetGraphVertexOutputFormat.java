/*******************************************************************************
 *    Licensed to the Apache Software Foundation (ASF) under one or more 
 *    contributor license agreements.  See the NOTICE file distributed with 
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with 
 *    the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package bigframe.datagen.nested.tweet;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class TweetGraphVertexOutputFormat extends
VertexOutputFormat<LongWritable, Text, NullWritable> {

  /** Uses the TextOutputFormat to do everything */
  protected TextOutputFormat<Text, Text> textOutputFormat =
      new TextOutputFormat<Text, Text>();

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    textOutputFormat.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return textOutputFormat.getOutputCommitter(context);
  }


  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex output.  Easiest to ignore the key value separator and only use
   * key instead.
   */
  protected abstract class TextVertexWriter
      extends VertexWriter<LongWritable, Text, NullWritable> {
    /** Internal line record writer */
    private RecordWriter<Text, Text> lineRecordWriter;
    /** Context passed to initialize */
    private TaskAttemptContext context;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
           InterruptedException {
      lineRecordWriter = createLineRecordWriter(context);
      this.context = context;
    }

    /**
     * Create the line record writer. Override this to use a different
     * underlying record writer (useful for testing).
     *
     * @param context
     *          the context passed to initialize
     * @return
     *         the record writer to be used
     * @throws IOException
     *           exception that can be thrown during creation
     * @throws InterruptedException
     *           exception that can be thrown during creation
     */
    protected RecordWriter<Text, Text> createLineRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
      return textOutputFormat.getRecordWriter(context);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      lineRecordWriter.close(context);
    }

    /**
     * Get the line record writer.
     *
     * @return Record writer to be used for writing.
     */
    public RecordWriter<Text, Text> getRecordWriter() {
      return lineRecordWriter;
    }

    /**
     * Get the context.
     *
     * @return Context passed to initialize.
     */
    public TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
   * A class to write a line for each
   * vertex.
   */
  private class TextVertexWriterToEachLine extends TextVertexWriter {

    /**
     * Writes a line for the given vertex.
     *
     * @param vertex
     *          the current vertex for writing
     * @return the text line to be written
     * @throws IOException
     *           exception that can be thrown while writing
     */
    protected  Text convertVertexToLine(Vertex<LongWritable, Text, NullWritable> vertex)
      throws IOException {
      
//      if (vertex.getValue()==null) {
//        System.out.println("Value is an empty string.");
//      }
//      else {
//        System.out.println("What is the value:" + vertex.getValue());
//      }
//      
      return vertex.getValue()==null ? null : vertex.getValue();
    }

    /* (non-Javadoc)
     * @see org.apache.giraph.io.SimpleVertexWriter#writeVertex(org.apache.giraph.graph.Vertex)
     */
    @Override
    public void writeVertex(Vertex<LongWritable, Text, NullWritable> vertex)
        throws IOException, InterruptedException {
      // Note we are writing line as key with null value
      if(convertVertexToLine(vertex) != null)
        getRecordWriter().write(convertVertexToLine(vertex), null);   
    }
  }

 
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TextVertexWriterToEachLine();
  }

//  /**
//   * Simple text based vertex writer
//   */
//  private class TweetGraphVertexWriter extends TextVertexWriterToEachLine {
//
//    @Override
//    protected Text convertVertexToLine(
//        Vertex<LongWritable, Text, NullWritable, ?> vertex) throws IOException {
//      // Output the set of tweet JSON
//      return vertex.getValue().equals("") ? null : vertex.getValue();
//    }
//
//  }

}
