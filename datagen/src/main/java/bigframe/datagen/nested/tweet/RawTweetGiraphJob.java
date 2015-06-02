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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenDriver;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.util.parser.JsonParser;

public class RawTweetGiraphJob {

  private static final Log LOG = LogFactory.getLog(RawTweetGiraphJob.class);

  private float targetGB;

  public static final int sizePerPartitionInGB = 1;

  // The average size of a tweet.
  public static float SINGLE_TWEET_INBYTES = 2380.445410414828f;

  private BigDataInputFormat conf;
  private GiraphConfiguration giraphConfig;
  private Path tweetGraphPath;
  private Path tweetOutputPath;

  public static List<JSONObject> tweetJSONs = new ArrayList<JSONObject>();
  static {
    InputStream tweetSampleFile;

    tweetSampleFile = DataGenDriver.class.getClassLoader().getResourceAsStream(
        "sample_tweet.json");

    BufferedReader in = new BufferedReader(new InputStreamReader(
        tweetSampleFile));

    try {
      String line;

      while ((line = in.readLine()) != null) {
        tweetJSONs.add(JsonParser.parseJsonFromString(line));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public RawTweetGiraphJob(GiraphConfiguration giraphConfig,
      BigDataInputFormat conf, float targetGB) {
    this.giraphConfig = giraphConfig;
    this.conf = conf;
    this.targetGB = targetGB;
    this.tweetGraphPath = new Path(conf.getDataStoredPath().get(
        BigConfConstants.BIGFRAME_DATA_HDFSPATH_GRAPH));
    this.tweetOutputPath = new Path(conf.getDataStoredPath().get(
        BigConfConstants.BIGFRAME_DATA_HDFSPATH_NESTED));

  }

  public boolean run() {
    LOG.info("Generating tweets......");

    try {

      GiraphJob job;

      job = new GiraphJob(giraphConfig, this.getClass().getName());

      GiraphConfiguration giraphConfiguration = job.getConfiguration();

      // We need to calculate how many tasks we need to spawn to generate the
      // target size.

      // There is a trade-off here: too many tasks will require a large number
      // of
      // map slots, but too few tasks will make each task quickly run out of
      // memory.
      float nestedProportion = conf.getDataScaleProportions().get(
          BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
      float graphProportion = conf.getDataScaleProportions().get(
          BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);

      float graphGB = graphProportion / nestedProportion * targetGB;

      int numTwitterUser = (int) KroneckerGraphGen.getNodeCount(graphGB);

      int maxNumberOfSupersteps = 20;

      // In GB
      float tweetSizeEachIteration = targetGB / maxNumberOfSupersteps;

      float tweetProb = tweetSizeEachIteration * 1024 * 1024 * 1024
          / (numTwitterUser * SINGLE_TWEET_INBYTES);

      // Guarantee the prob <= 1.
      while(tweetProb > 1) {
        tweetProb /= 2;
        maxNumberOfSupersteps *= 2;
      }
      
      LOG.info("Tweet size per interation: " + tweetSizeEachIteration);
      LOG.info("Tweet prob:" + tweetProb);

      //
      int numWorkers = (int) Math.ceil(targetGB + graphGB);

      LOG.info("Number of workers: " + numWorkers);

      // giraphConfiguration.setMaxNumberOfSupersteps(maxNumberOfSupersteps);
      giraphConfiguration.setInt(RawTweetGenConstants.SUPERSTEP_COUNT,
          maxNumberOfSupersteps);
      giraphConfiguration.setFloat(RawTweetGenConstants.TWEET_PROB, tweetProb);
      /**
       * Initialize vertex and edge input
       */
      GiraphFileInputFormat.addVertexInputPath(giraphConfiguration, new Path(
          RawTweetGenConstants.PREPARED_VERTEX_HDFS_PATH));
      GiraphFileInputFormat.addEdgeInputPath(giraphConfiguration,
          tweetGraphPath);
      giraphConfiguration
          .setVertexInputFormatClass(TweetGraphVertexInputFormat.class);
      giraphConfiguration
          .setEdgeInputFormatClass(TweetGraphEdgeInputFormat.class);

      /**
       * Initialize vertex output
       */
      FileSystem fs = FileSystem.get(giraphConfiguration);
      Path output_path = tweetOutputPath;

      if (fs.exists(output_path))
        fs.delete(output_path, true);

      FileOutputFormat.setOutputPath(job.getInternalJob(), output_path);
      giraphConfiguration
          .setVertexOutputFormatClass(TweetGraphVertexOutputFormat.class);
      /**
       * Set computation class
       */
      giraphConfiguration.setComputationClass(TweetGraphVertex.class);
      giraphConfiguration
          .setWorkerConfiguration(numWorkers, numWorkers, 100.0f);

      giraphConfiguration.set("giraph.useOutOfCoreGraph", "true");

      return job.run(true) ? true : false;

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return false;

  }

}
