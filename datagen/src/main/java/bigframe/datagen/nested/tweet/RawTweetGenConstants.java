package bigframe.datagen.nested.tweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Some constants used by the raw tweet generator.
 * 
 * @author andy
 * 
 */
public class RawTweetGenConstants {
  public static final String NUM_MAPPERS = "mapreduce.rawtweet.num-mappers";
  public static final String NUM_NODES = "mapreduce.rawtweet.num-nodes";
  public static final String TWEETS_PER_DAY = "mapreduce.rawtweet.tweet-per-day";
  public static final String TWEETS_PER_MAPPER = "mapreduce.rawtweet.tweet-per-mapper";
  public static final String TIME_BEGIN = "mapreduce.rawtweet.time-begin";
  public static final String TIME_END = "mapreduce.rawtweet.time-end";

  public static final String TWEETGEN_NAME = "mapreduce.rawtweet.textgen";
  public static final String TWEET_OUTPUT_PATH = "mapreduce.rawtweet.output.path";

  public static final String TPCDS_TARGET_GB = "mapreduce.rawtweet.tpcds-target-GB";
  public static final String GRAPH_TARGET_GB = "mapreduce.rawtweet.graph-target-GB";
  // public static final String NESTED_TARGET_GB =
  // "mapreduce.rawtweet.nested-targetGB";

  public static final String NUM_PRODUCT = "mapreduce.rawtweet.num-product";
  public static final String NUM_TWITTER_USER = "mapreduce.rawtweet.num-twitter-user";

  public static final String SUPERSTEP_COUNT = "giraph.num.superstep";
  
  public static final String BIGFRAME_TWEET_CUST_PORTION = "bigframe.tweet.cust.portion";
  // The prob that the mentioned product is a promoted one
  public static final String BIGFRAME_TWEET_PROMOTED_PROD_PORTION = "bigframe.tweet.promoted.prod.portion";
  // THe prob that mention the product in its promotion period
  public static final String BIGFRAME_TWEET_PROMOTION_PERIOD_PROB = "bigframe.tweet.promotion.period.prob";

  public static final String PROMOTION_TBL = "promotion";
  public static final String ITEM_TBL = "item";

  public static final String SPLIT_STR = "{|}";
  public static final String PREPARED_VERTEX_HDFS_PATH = "prepared_tweet_vertex";



  public static final String SAMPLE_TWEET_PATH = "bigframe.sample.tweet.path";
  public static final String SAMPLE_TWEET_HDFS_PATH = "tweet.json";

  public static final int GB_PER_MAPPER = 1;

  // All promoted items are covered by this range.
  public static String TWEET_BEGINDATE = "1996-01-01";
  public static String TWEET_ENDDATE = "1998-12-31";
  
  public static SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
      "EEE MMM dd HH:mm:ss ZZZZZ yyyy");
  static {
    twitterDateFormat.setTimeZone(TimeZone.getTimeZone("Europe/London"));
  }
  
  public static float TWEET_PROB = 0.01f;
  
  public static float INIT_PROB = 0.01f;
}
