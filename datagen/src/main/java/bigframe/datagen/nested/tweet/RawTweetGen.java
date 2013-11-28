package bigframe.datagen.nested.tweet;

import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenDriver;
import bigframe.datagen.nested.NestedDataGen;




/**
 * Abstract class for all raw tweet generator.
 * 
 * @author andy
 * 
 */
public abstract class RawTweetGen extends NestedDataGen {
	protected InputStream tweet_template_file;
	protected String textgen_name;

	protected String hdfs_dir;
	

	public RawTweetGen(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);

		// TODO Auto-generated constructor stub
		tweet_template_file = DataGenDriver.class.getClassLoader().getResourceAsStream("tweet_template.json");

		textgen_name = "simple";

		hdfs_dir = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_NESTED);


	}

	protected  Date stringToDate(String date) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		try {
			return formatter.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	protected int daysBetween(Date d1, Date d2){
		return (int)( (d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
	}


	public long getTotalNumTweets() {		
		long targetByte = (long) (targetGB*1024*1024*1024);

		return targetByte / RawTweetGenConstants.SINGLE_TWEET_INBYTES;
	}
	
	public long getNumTweetsBySize(int sizeInGB) {
		return sizeInGB*1024*1024*1024/RawTweetGenConstants.SINGLE_TWEET_INBYTES; 
	}
	

	public long getTweetsPerDay(int days_between) {
		long tweets_per_day = 0;

		long targetByte = (long) (targetGB*1024*1024*1024);

		tweets_per_day = (long) (targetByte*1.0/days_between/RawTweetGenConstants.SINGLE_TWEET_INBYTES);

		if(tweets_per_day <=0) {
			System.out.println("Tweets sent per day is less than 0, please increase the data volumn " +
					"or increase the proportion of nested data!");
			System.exit(-1);
		}

		return tweets_per_day;
	}

	public void setTextGenName(String name) {
		textgen_name = name;
	}

}
