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
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.datagen.relational.tpcds.CollectTPCDSstat;
import bigframe.datagen.relational.tpcds.CollectTPCDSstatNaive;
import bigframe.datagen.relational.tpcds.TpcdsConstants;
import bigframe.datagen.relational.tpcds.TpcdsItemInfo;
import bigframe.datagen.relational.tpcds.TpcdsPromotionInfo;
import bigframe.datagen.util.HDFSUtil;
import bigframe.util.MapRedConfig;

/* Generate a set of seed vertexes used by the true tweet generator later on.
 *  Each vertex has the following format:
 *    NodeID | Tweet Template | productName | promotion start date | promotion end date
 *    
 */
public class RawTweetGiraphPrepare {
  
  // Average size of a seed vertex
  public static final int SINGLE_SEED_VERTEX_INBYTES = 2975;
  private static final Log LOG = LogFactory.getLog(RawTweetGiraphPrepare.class);

  private BigDataInputFormat conf;
  private float targetGB;

  public RawTweetGiraphPrepare(BigDataInputFormat conf, float targetGB) {
    this.conf = conf;
    this.targetGB = targetGB;
  }

  public long getNumUsersBySize(int sizeInGB) {
    return sizeInGB * 1024 * 1024 * 1024
        / SINGLE_SEED_VERTEX_INBYTES;  
  }

  public void prepare() {
    LOG.info("Preparing for tweet generation......");

    // Calculate the number twitter account based on the graph volume in GB
    float nested_proportion = conf.getDataScaleProportions().get(
        BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
    float twitter_graph_proportion = conf.getDataScaleProportions().get(
        BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
    float tpcds_proportion = conf.getDataScaleProportions().get(
        BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);

    float graph_targetGB = twitter_graph_proportion / nested_proportion
        * targetGB;
    float tpcds_targetGB = tpcds_proportion / nested_proportion * targetGB;

    CollectTPCDSstatNaive tpcds_stat_collecter = new CollectTPCDSstatNaive();

    // The numbers of promotion and items are decided by the scale of the tpcds
    // data.
    // This is calculated by the above proportion.
    tpcds_stat_collecter.genTBLonHDFS(conf, tpcds_targetGB,
        RawTweetGenConstants.PROMOTION_TBL);
    tpcds_stat_collecter.genTBLonHDFS(conf, tpcds_targetGB,
        RawTweetGenConstants.ITEM_TBL);

    int num_twitter_user = (int) KroneckerGraphGen.getNodeCount(graph_targetGB);

    String tweetSamplePath = conf.get(RawTweetGenConstants.SAMPLE_TWEET_PATH);
    String tweetHdfsPath = RawTweetGenConstants.SAMPLE_TWEET_HDFS_PATH;

    Configuration mapred_config = MapRedConfig.getConfiguration(conf);
    HDFSUtil.copyFromLocal2HDFS(mapred_config, tweetSamplePath, tweetHdfsPath);

    int GBPerMapper = RawTweetGenConstants.GB_PER_MAPPER;
    long user_per_mapper = getNumUsersBySize(GBPerMapper);

    int num_Mapper = (int) Math.ceil(num_twitter_user * 1.0 / user_per_mapper);

    mapred_config.setLong(RawTweetGenConstants.NUM_TWITTER_USER,
        num_twitter_user);
    mapred_config.setInt(RawTweetGenConstants.NUM_MAPPERS, num_Mapper);

    float probMentionPromotedProd = Float.parseFloat(conf.get(
        RawTweetGenConstants.BIGFRAME_TWEET_PROMOTED_PROD_PORTION, "0.2"));

    LOG.info("Number of twitter User:" + num_twitter_user);
    LOG.info("Number of mappers:" + num_Mapper);
    LOG.info("Number users per mapper:" + user_per_mapper);
    
    
    mapred_config.setFloat(
        RawTweetGenConstants.BIGFRAME_TWEET_PROMOTED_PROD_PORTION,
        probMentionPromotedProd);
    mapred_config.setInt(RawTweetGenConstants.NUM_TWITTER_USER, num_twitter_user);
    mapred_config.setLong(RawTweetGenConstants.NUM_MAPPERS, num_Mapper);
    // long dateBegin_time_sec = dateBegin.getTime() / 1000;
    // long dateEnd_time_sec = dateEnd.getTime() / 1000;

    try {
      DistributedCache.addCacheFile(new URI(RawTweetGenConstants.PROMOTION_TBL
          + ".dat"), mapred_config);
      DistributedCache.addCacheFile(new URI(RawTweetGenConstants.ITEM_TBL
          + ".dat"), mapred_config);
      DistributedCache.addCacheFile(new URI(
          RawTweetGenConstants.SAMPLE_TWEET_HDFS_PATH), mapred_config);
    } catch (URISyntaxException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    try {
      Job job = new Job(mapred_config);

//      HDFSUtil.deleteFileOnHDFS(mapred_config,
//          RawTweetGenConstants.PREPARED_VERTEX_HDFS_PATH);
      Path outputDir = new Path(RawTweetGenConstants.PREPARED_VERTEX_HDFS_PATH);

      FileOutputFormat.setOutputPath(job, outputDir);
      job.setJarByClass(RawTweetPrepareMapper.class);
      job.setMapperClass(RawTweetPrepareMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(SeedVertexInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.waitForCompletion(true);

      // Encounter error when the _logs directory exists
      HDFSUtil.deleteFileOnHDFS(mapred_config,
          RawTweetGenConstants.PREPARED_VERTEX_HDFS_PATH + "/_logs");
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  // The Mapper for generate seed vertexes
  static class RawTweetPrepareMapper extends
      Mapper<NullWritable, SeedVertexInfoWritable, LongWritable, Text> {

    private List<String> initTweetSample(BufferedReader in) {
      List<String> tweets = new ArrayList<String>();

      try {
        String line;

        while ((line = in.readLine()) != null) {
          tweets.add(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return tweets;
    }

    @Override
    protected void map(NullWritable ignored,
        SeedVertexInfoWritable seedVertexInput, final Context context)
        throws IOException, InterruptedException {

      long startID = seedVertexInput.startID;
      long endID = seedVertexInput.endID;

      Configuration mapreduce_config = context.getConfiguration();

      CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();

      TpcdsPromotionInfo promt_info = new TpcdsPromotionInfo();
      TpcdsItemInfo item_info = new TpcdsItemInfo();
      List<String> tweets = new ArrayList<String>();
      Path[] uris = DistributedCache.getLocalCacheFiles(mapreduce_config);
      for (int i = 0; i < uris.length; i++) {
        BufferedReader in = new BufferedReader(new FileReader(
            uris[i].toString()));
        if (uris[i].toString().contains(RawTweetGenConstants.PROMOTION_TBL)) {
          tpcds_stat_collecter.setPromtResult(in, promt_info);
        } else if (uris[i].toString().contains(RawTweetGenConstants.ITEM_TBL)) {
          tpcds_stat_collecter.setItemResult(in, item_info);
        } else if (uris[i].toString().contains(
            RawTweetGenConstants.SAMPLE_TWEET_HDFS_PATH)) {
          tweets = initTweetSample(in);
        }
      }

      // Use startID as the seed.
      Random random = new Random(startID);
      int promtionSize = promt_info.getProductSK().size();
      int itemSize = item_info.getItemIDs().size();

      float probMentionPromotedProd = mapreduce_config.getFloat(
          RawTweetGenConstants.BIGFRAME_TWEET_PROMOTED_PROD_PORTION, 0.2f);

      for (long i = startID; i <= endID; i++) {
        int productID;
        String prodName;
        String period;

        // Mention a promoted product
        if (random.nextFloat() < probMentionPromotedProd) {
          int index1 = random.nextInt(promtionSize);
          productID = promt_info.getProductSK().get(index1);

          int dateBegin = promt_info.getDateBeginSK().get(index1);
          int dateEnd = promt_info.getDateEndSK().get(index1);

          Date begin = CollectTPCDSstat.getDateBySK(dateBegin);
          Date end = CollectTPCDSstat.getDateBySK(dateEnd);

          period = TpcdsConstants.dateformatter.format(begin)
              + "|" + TpcdsConstants.dateformatter.format(end);
        }

        // Mention a whatever product
        else {
          productID = random.nextInt(itemSize) + 1;
          period = RawTweetGenConstants.TWEET_BEGINDATE + "|"
              + RawTweetGenConstants.TWEET_ENDDATE;
        }

        // The product name and the product id have one-to-one relationship
        prodName = item_info.getProdName().get(productID - 1);

//        context.write(new LongWritable(i), new Text(tweet
//            + RawTweetGenConstants.SPLIT_STR + prodName  + RawTweetGenConstants.SPLIT_STR + period));
        context.write(new LongWritable(i), new Text(prodName  + "|" + period));
      }

    }
  }

}
