package bigframe.datagen.nested;

import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import bigframe.BigFrameDriver;
import bigframe.datagen.DatagenConf;
import bigframe.datagen.text.TweetTextGen;
import bigframe.datagen.text.TweetTextGenSimple;
import bigframe.util.Constants;


public abstract class RawTweetGen extends NestedDataGen {
	protected InputStream tweet_template_file;
	protected TweetTextGen textgen;
	
	protected String hdfs_dir;
	private final int single_tweet_inByte = 1379;
	
	public RawTweetGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		
		// TODO Auto-generated constructor stub
		tweet_template_file = BigFrameDriver.class.getClassLoader().getResourceAsStream("tweet_template.json");
		textgen = new TweetTextGenSimple(conf, 0);
		
		hdfs_dir = conf.getDataStoredPath().get(Constants.BIGFRAME_DATA_HDFSPATH_NESTED);
		
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
	
	public long getTweetsPerDay(int days_between) {
		long tweets_per_day = 0;
		
		
		
		long targetByte = (long) (targetGB*1024*1024*1024);
		
		tweets_per_day = (long) (targetByte*1.0/days_between/single_tweet_inByte);
		
		if(tweets_per_day <=0) {
			System.out.println("Tweets sent per day is less than 0, please increase the data volumn " +
						"or increase the proportion of nested data!");
			System.exit(-1);
		}
		
		return tweets_per_day;
	}
	
	public void setTextGen(TweetTextGen tg) {
		textgen = tg;
	}

}
