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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TweetGraphEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, NullWritable> {

  @Override
  public EdgeReader<LongWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new TweetGraphEdgeReader();
  }

  public class TweetGraphEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<DirectEdge> {

    @Override
    protected LongWritable getSourceVertexId(DirectEdge edge)
        throws IOException {
//      System.out.println("getSrcNode: " + edge.getSrcNode());
      return edge.getSrcNode();
    }

    @Override
    protected LongWritable getTargetVertexId(DirectEdge edge)
        throws IOException {
//      System.out.println("getDestNode: " + edge.getDestNode());
      return edge.getDestNode();
    }

    @Override
    protected NullWritable getValue(DirectEdge edge) throws IOException {
//      System.out.println("NullWritable: " + NullWritable.get());
      return NullWritable.get();
    }

    @Override
    protected DirectEdge preprocessLine(Text line) throws IOException {
      String[] fields = line.toString().split("\\|");

      
      try {
        return new DirectEdge(new LongWritable(Long.parseLong(fields[0])),
            new LongWritable(Long.parseLong(fields[1])));
      } catch (ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        return null;
      }
    }

  }

}
