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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import bigframe.datagen.relational.tpcds.TpcdsConstants;
import bigframe.datagen.text.tweet.TweetTextGen;
import bigframe.datagen.text.tweet.TweetTextGenSimple;
import bigframe.datagen.util.RandomUtil;

//Vertex<I, V, E, M>
//<I> Vertex id
//<V> Vertex data
//<E> Edge data
//<M> Message data
public class TweetGraphVertex extends
    BasicComputation<LongWritable, Text, NullWritable, Text> {

  public static enum Vertex {
    INITIALIZED,
    PORPOGATE,
    TWEET
  };

  // Set the seed as the vertex ID.
  // private Random random;
  // private Random random;
  private String lastProductName;
  private String startDate;
  private String endDate;
  private int tweetID;
  // private int numSuperSteps;
  private float mentionInPeriodProb;

  private static TweetTextGen tweetGen = new TweetTextGenSimple(null, 0);

  public void sendMessageToAllEdges(
      org.apache.giraph.graph.Vertex<LongWritable, Text, NullWritable> vertex,
      Text message) {

    Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();

    for (Edge<LongWritable, NullWritable> edge : edges) {
      sendMessage(edge.getTargetVertexId(), message);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph
   * .Vertex, java.lang.Iterable)
   */
  @Override
  public void compute(
      org.apache.giraph.graph.Vertex<LongWritable, Text, NullWritable> vertex,
      Iterable<Text> messages) throws IOException {
    // initialize the necessary parameters
    Random random = new Random(vertex.getId().get() + getSuperstep());
    int numSuperSteps = getContext().getConfiguration().getInt(
        RawTweetGenConstants.SUPERSTEP_COUNT, 1);
    if (getSuperstep() == 0) {
      // random = new Random(vertex.getId().get());
      getContext().getCounter(Vertex.INITIALIZED).increment(1);

      String[] fields = vertex.getValue().toString().split("\\|");

      mentionInPeriodProb = getContext().getConfiguration().getFloat(
          RawTweetGenConstants.BIGFRAME_TWEET_PROMOTION_PERIOD_PROB, 0.8f);

      lastProductName = fields[0];
      startDate = fields[1];
      endDate = fields[2];

      tweetID = random.nextInt(RawTweetGiraphJob.tweetJSONs.size());

      if (random.nextFloat() <= RawTweetGenConstants.INIT_PROB) {
        // Pass this product to its followers.
        Text message = new Text(lastProductName + "|" + startDate + "|"
            + endDate);
        sendMessageToAllEdges(vertex, message);
      }
      vertex.setValue(null);
      // voteToHalt();
    }
    //
    // Begin to generate tweets.
    else if (getSuperstep() <= numSuperSteps) {
      getContext().getCounter(Vertex.PORPOGATE).increment(1);

      // Tweet about this product
      if (random.nextFloat() <= getContext().getConfiguration().getFloat(
          RawTweetGenConstants.TWEET_PROB, 0.0001f)) {

        getContext().getCounter(Vertex.TWEET).increment(1);

        List<String> products = new ArrayList<String>();
        List<String> startDates = new ArrayList<String>();
        List<String> endDates = new ArrayList<String>();

        for (Text message : messages) {
          String[] fields = message.toString().split("\\|");
          products.add(fields[0]);
          startDates.add(fields[1]);
          endDates.add(fields[2]);
        }

        // This user has friends
        if (products.size() > 0) {
          // Randomly select a product tweet by his/her friends.
          // For simplicity, we assume that friends have the same
          // influence on the same follower.
          int index = random.nextInt(products.size());

          // Update the product he/she will mention.
          lastProductName = products.get(index);
          startDate = startDates.get(index);
          endDate = endDates.get(index);
        }

        long startTime = 0;
        long endTime = 0;
        // Tweet this product in its specified period
        if (random.nextFloat() <= mentionInPeriodProb) {
          try {
            startTime = TpcdsConstants.dateformatter.parse(startDate).getTime();
            endTime = TpcdsConstants.dateformatter.parse(endDate).getTime();
          } catch (ParseException e) {
            e.printStackTrace();
          }
        }

        // Tweet this product in whatever timestamp between the predefined begin
        // and end
        else {
          try {
            startTime = TpcdsConstants.dateformatter.parse(
                RawTweetGenConstants.TWEET_BEGINDATE).getTime();
            endTime = TpcdsConstants.dateformatter.parse(
                RawTweetGenConstants.TWEET_ENDDATE).getTime();
          } catch (ParseException e) {
            e.printStackTrace();
          }
        }

        // Begin to construct the tweet body
        long tweetTime = RandomUtil.randLong(random, startTime, endTime);

        String date = RawTweetGenConstants.twitterDateFormat.format(tweetTime);
        // The product id is not used in the current implementation.
        String tweet = tweetGen.getNextTweet(0);

        JSONObject tweetJSON = RawTweetGiraphJob.tweetJSONs.get(tweetID);
        JSONObject userJSON = (JSONObject) tweetJSON.get("user");
        JSONObject entitiesJSON = (JSONObject) tweetJSON.get("entities");

        tweetJSON.put("created_at", date);
        tweetJSON.put("text", tweet);

        userJSON.put("id", vertex.getId().get());
        userJSON.put("id_str", vertex.getId().toString());
        tweetJSON.put("user", userJSON);

        // TODO: Shall we tweet N products instead of one?
        JSONArray list = new JSONArray();
        list.add(lastProductName);
        entitiesJSON.put("hashtags", list);

        tweetJSON.put("entities", entitiesJSON);

        // Append to the value, which is a list of tweets by this user.
        if (vertex.getValue() == null) {
          vertex.setValue(new Text(tweetJSON.toString()));
        } else {
          vertex.setValue(new Text(vertex.getValue().toString() + "\n"
              + tweetJSON.toString()));
        }

        // Pass this product to its followers.
        Text message = new Text(lastProductName + "|" + startDate + "|"
            + endDate);

        sendMessageToAllEdges(vertex, message);
      }
    } else
      vertex.voteToHalt();

  }

}
