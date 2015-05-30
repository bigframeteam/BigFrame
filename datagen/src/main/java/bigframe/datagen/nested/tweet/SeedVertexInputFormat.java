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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import bigframe.datagen.graph.GraphCellInputFormat;
import bigframe.datagen.graph.kroneckerGraph.KnonGraphConstants;
import bigframe.datagen.graph.kroneckerGraph.KronGraphInfoWritable;


/**
 * Class for recording the range of vertex ids that a Mapper need to
 * handle.
 * 
 * @author andy
 * 
 */
public class SeedVertexInputFormat extends InputFormat<NullWritable, SeedVertexInfoWritable> {

  private static final Logger LOG = Logger
      .getLogger(SeedVertexInputFormat.class);
  
  static class SeedVertexInputSplit extends InputSplit implements Writable {
    public long startID;
    public long endID;

    public SeedVertexInputSplit() {
    }

    public SeedVertexInputSplit(long startID, long endID) {
      this.startID = startID;
      this.endID = endID;
    }

    public long getLength() throws IOException {
      return 0;
    }

    public String[] getLocations() throws IOException {
      return new String[] {};
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      // TODO Auto-generated method stub
      startID = WritableUtils.readVLong(in);
      endID = WritableUtils.readVLong(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // TODO Auto-generated method stub
      WritableUtils.writeVLong(out, startID);
      WritableUtils.writeVLong(out, endID);
    }
  }

  static class SeedVertexRecordReader extends
  RecordReader<NullWritable, SeedVertexInfoWritable> {

    private static final Logger LOG = Logger
        .getLogger(SeedVertexRecordReader.class);

    long startID;
    long endID;
    
    NullWritable key = null;
    SeedVertexInfoWritable value = null;

    public SeedVertexRecordReader() {
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {

      this.startID = ((SeedVertexInputSplit) split).startID;
      this.endID = ((SeedVertexInputSplit) split).endID;

    }

    public void close() throws IOException {
      // NOTHING
    }

    public NullWritable getCurrentKey() {
      return key;
    }

    public SeedVertexInfoWritable getCurrentValue() {
      return value;
    }

    public float getProgress() throws IOException {
      return 0;
    }

    public boolean nextKeyValue() {
      if (key == null && value == null) {
        value = new SeedVertexInfoWritable(startID, endID);
        return true;
      } else
        return false;
    }

  }
  
  @Override
  public RecordReader<NullWritable, SeedVertexInfoWritable> createRecordReader(
      InputSplit arg0, TaskAttemptContext arg1) throws IOException,
      InterruptedException {
    return new SeedVertexRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException,
      InterruptedException {
    int numMappers = job.getConfiguration().getInt(
        RawTweetGenConstants.NUM_MAPPERS, 1);
    
    long numNodes = job.getConfiguration().getLong(
        RawTweetGenConstants.NUM_TWITTER_USER, 1L);
    
    long nodePerMapper = numNodes / numMappers;
    
    LOG.info("Number of mapper: " + numMappers);
    LOG.info("Number of users: " + numNodes);
    
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    int i = 0;
    for(; i < numMappers - 1; i++) {
      // In the graph generation, we add an offset such that size in byte for 
      // each node is consistent.
      long startID = 1 + i * nodePerMapper + KnonGraphConstants.OFFSET;
      long endID = (i + 1) * nodePerMapper + KnonGraphConstants.OFFSET;
      
      InputSplit split = new SeedVertexInputSplit(startID, endID);
      splits.add(split);
    }
    
    // The last mapper handles the rest
    long startID = 1 + i * nodePerMapper + KnonGraphConstants.OFFSET;
    long endID = numNodes + KnonGraphConstants.OFFSET;
    InputSplit split = new SeedVertexInputSplit(startID, endID);
    splits.add(split);
    
    return splits;
  }

}
