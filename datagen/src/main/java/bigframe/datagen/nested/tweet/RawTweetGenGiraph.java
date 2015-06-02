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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.conf.Configuration;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.util.MapRedConfig;

/**
 * This program generates the tweets according to the assumption of TwitterRank:
 * friends will influence what their followers tweet.
 * 
 */
public class RawTweetGenGiraph extends RawTweetGen {

  /**
   * @param conf
   * @param targetGB
   */
  public RawTweetGenGiraph(BigDataInputFormat conf, float targetGB) {
    super(conf, targetGB);
    // TODO Auto-generated constructor stub
  }

  // ********************************************************************
  // Need to start several round of Giraph job, since launch too
  // many task could jeopardise the performance a lot.
  //
  // Besides, we need to guarantee that the random seeds are consistent
  // everytime that the generate function is called.
  //
  // ********************************************************************
  @Override
  public void generate() {
    RawTweetGiraphPrepare prepareJob = new RawTweetGiraphPrepare(conf, targetGB);
    prepareJob.prepare();

    Configuration mapredConfig = MapRedConfig.getConfiguration(conf);
    GiraphConfiguration giraphConfig = new GiraphConfiguration(mapredConfig);
    Configuration.addDefaultResource("giraph-site.xml");

    RawTweetGiraphJob tweetGenJob = new RawTweetGiraphJob(giraphConfig, conf, targetGB);
    tweetGenJob.run();
  }

  /*
   * (non-Javadoc)
   * 
   * @see bigframe.datagen.DataGenerator#getAbsSizeBySF(int)
   */
  @Override
  public int getAbsSizeBySF(int sf) {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see bigframe.datagen.DataGenerator#getSFbyAbsSize(int)
   */
  @Override
  public int getSFbyAbsSize(int absSize) {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see bigframe.datagen.nested.tweet.RawTweetGen#getTotalNumTweets()
   */
  @Override
  public long getTotalNumTweets() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see bigframe.datagen.nested.tweet.RawTweetGen#getNumTweetsBySize(int)
   */
  @Override
  public long getNumTweetsBySize(int sizeInGB) {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see bigframe.datagen.nested.tweet.RawTweetGen#getTweetsPerDay(int)
   */
  @Override
  public long getTweetsPerDay(int days_between) {
    // TODO Auto-generated method stub
    return 0;
  }

}
