package bigframe.spark

import spark.RDD
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
	  tweetDateFormat.setLenient(true)
	  tpcdsDateFormat.setLenient(true)   
    }
   
    /**
     * Converts date parsed from tweet to java date
     */
    private def fromTweetToDate(date_string: String): Date = {
    	val date = try {
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
        to_date_string: String, tpcds_dates: RDD[(String, String)]):Boolean = {
      val tweet_date = fromTweetToDate(tweet_date_string)
      val from_date = try {
        fromTpcdsToDate(tpcds_dates.filter(
            t => t._1.equals(from_date_string)).map(t => t._2).first())
      } catch {
        case e: Exception => new Date(0)
      }
      val to_date = try {
        fromTpcdsToDate(tpcds_dates.filter(
            t => t._1.equals(to_date_string)).map(t => t._2).first())
      } catch {
        case e: Exception => new Date(0)
      }
//      val to_date = fromTpcdsToDate(tpcds_dates.getOrElse(to_date_string, "0000-00-00"))
      (tweet_date after from_date) & (tweet_date before to_date)
    }

}