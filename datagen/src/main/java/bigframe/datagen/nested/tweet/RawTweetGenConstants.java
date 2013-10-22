package bigframe.datagen.nested.tweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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
	public static final String TWEETS_PER_DAY = "mapreduce.rawtweet.tweet-per-day";
	public static final String TWEETS_PER_MAPPER = "mapreduce.rawtweet.tweet-per-mapper";
	public static final String TIME_BEGIN = "mapreduce.rawtweet.time-begin";
	public static final String TIME_END = "mapreduce.rawtweet.time-end";


	public static final String TWEETGEN_NAME = "mapreduce.rawtweet.textgen";


	public static final String TPCDS_TARGET_GB = "mapreduce.rawtweet.tpcds-target-GB";
	public static final String GRAPH_TARGET_GB = "mapreduce.rawtweet.graph-target-GB";
	//public static final String NESTED_TARGET_GB = "mapreduce.rawtweet.nested-targetGB";

	public static final String NUM_PRODUCT = "mapreduce.rawtweet.num-product";
	public static final String NUM_TWITTER_USER = "mapreduce.rawtweet.num-twitter-user";

	public static final String PROMOTION_TBL = "promotion";
	public static final String ITEM_TBL = "item";
	public static final int SINGLE_TWEET_INBYTES = 1379;
	//public static final String TWEET_TEMPLATE = "tweet.json";
	//public static final String SENTIMENT_DICT = "sentiment.dict";


	// public static final TweetTextGenSimple TEXT_GEN = new
	// TweetTextGenSimple(null, 0);
	//public static final InputStream TWEET_TEMPLATE_FILE = DataGenDriver.class.getClassLoader().getResourceAsStream("tweet_template.json");
	//public static final JSONObject TWEET_JSON = parseJsonFromFile(TWEET_TEMPLATE_FILE);

	public static final int GB_PER_MAPPER = 1;

	public static String TWEET_BEGINDATE = "1997-01-01";
	public static String TWEET_ENDDATE = "1998-01-01";

//	public static JSONObject parseJsonFromFile(InputStream file) {
//		JSONParser parser = new JSONParser();
//		try {
//
//			Object obj = parser.parse(new BufferedReader(new InputStreamReader(file)));
//
//			JSONObject jsonObject = (JSONObject) obj;
//
//			return jsonObject;
//
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		return null;
//	}
}
