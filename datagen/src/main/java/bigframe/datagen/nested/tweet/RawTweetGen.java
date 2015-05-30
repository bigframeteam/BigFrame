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
import bigframe.datagen.relational.tpcds.TpcdsConstants;




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

		try {
			return TpcdsConstants.dateformatter.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	protected int daysBetween(Date d1, Date d2){
		return (int)( (d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
	}


	abstract public long getTotalNumTweets();
	
	abstract public long getNumTweetsBySize(int sizeInGB);

	abstract public long getTweetsPerDay(int days_between);

	public void setTextGenName(String name) {
		textgen_name = name;
	}

}
