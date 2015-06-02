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

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TweetGraphVertexInputFormat<E extends Writable, M extends Writable>
    extends TextVertexValueInputFormat<LongWritable, Text, E> {

  @Override
  public TextVertexValueReader createVertexValueReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new TweetGraphVertexValueReader();
  }

  public class TweetGraphVertexValueReader extends
      TextVertexValueReaderFromEachLineProcessed<SeedVertex> {

    @Override
    protected SeedVertex preprocessLine(Text line) throws IOException {
      String[] fields = line.toString().split("\\t");

      try {
      LongWritable id = new LongWritable(Long.parseLong(fields[0]));
      Text value = new Text(fields[1]);

      return new SeedVertex(id, value);
      } catch (ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    protected LongWritable getId(SeedVertex data) throws IOException {
//      System.out.println("getID: " + data.getId());
      return data.getId();
    }

    @Override
    protected Text getValue(SeedVertex data) throws IOException {
//      System.out.println("getValue: " + data.getValue());
      return data.getValue();
    }
  }

}
