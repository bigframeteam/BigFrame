package bigframe.queries.BusinessIntelligence.macro.exploratory

import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Making the class serializable.
 * Make sure it doesn't have any parameter that is not serializable like SparkContext 
 */
class DateUtils extends Serializable {
    final val tweetDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", 
        java.util.Locale.ENGLISH)
    final val tpcdsDateFormat = new SimpleDateFormat("yyyy-MM-dd", 
        java.util.Locale.ENGLISH) 
  
    def apply() = {
      println("Apply in DateUtils")
	  tweetDateFormat.setLenient(true)
	  tpcdsDateFormat.setLenient(true)   
    }
   
    /**
     * Converts date parsed from tweet to java date
     */
    private def fromTweetToDate(date_string: String): Date = {
    	val date = try {
    	  println("Parsing tweet date: " + date_string)
    	  return tweetDateFormat.parse(date_string)
    	} catch {
    	  case e: Exception => println(e) 
    	}
    	new Date(0)
    }
    
    /**
     * Converts date parsed from tpcds data to java date
     */
    private def fromTpcdsToDate(date_string: String): Date = {
    	val date = try {
    	  println("Parsing TPCDS date: " + date_string)
    	  return tpcdsDateFormat.parse(date_string)
    	} catch {
    	  case e: Exception => println(e) 
    	}      
    	new Date(0)
    }
  
    /**
     * Returns true if tweet is created within promotion duration
     */
    def isDateWithin(tweet_date_string: String, from_date_string: String, 
        to_date_string: String): Boolean = {
      println("Trying to find whether " + tweet_date_string + " is within " + from_date_string + " and " + to_date_string)
      val tweet_date = fromTweetToDate(tweet_date_string)
      val from_date = fromTpcdsToDate(from_date_string)
      val to_date = fromTpcdsToDate(to_date_string)

      (tweet_date after from_date) & (tweet_date before to_date)
    }

}