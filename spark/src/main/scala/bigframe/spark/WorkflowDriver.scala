package bigframe.spark

import spark.SparkContext
import SparkContext._

/*
Class to run entire workflow
*/
object WorkflowDriver {

   /*
    Arguments:
     1. Spark connection string -- "spark://<ip/name>:7070"
     2. TPCDS path -- "hdfs://<ip/name>:9000/<path_to_TPCDS>"
     3. Tweet file(s) path -- "hdfs://<ip/name>:9000/<path_to_file>"
   */
   def main(args: Array[String]) {
       if(args.length < 3)
       {
         println("Arguments:\n 1. Spark connection string -- \"spark://<ip/name>:7070\"\n 2. TPCDS path -- \"hdfs://<ip/name>:<hdfs port>/<path_to_TPCDS>\"\n 3. Tweet file(s) path -- \"hdfs://<ip/name>:<hdfs port>/<path_to_file>\"")
         System.exit(0)
       }
 
       val sc = new SparkContext(args(0), "BigFrame",
          System.getenv("SPARK_HOME"), Seq(System.getenv("BIGFRAME_JAR")))
       println("Set the context: " + sc)

       // relational part
       val relational = new Relational()
       relational.init(sc)
       relational.process(args(1))

       // unstructured part
       val reader = new TweetReader()
       reader.init(sc)
       val tweets = reader.read(args(2))
       // run sentiment analysis
       //to test: val tweets = Array( new Tweet("1 I abandoned study", "today"), new Tweet("2 Feeling proud", "today"))
       val tweetsWithSentiment = reader.addSentimentScore(tweets)
   
       println("tweets: ")
       tweetsWithSentiment foreach {a => println(a)}

       //TODO: Combine sentiment score with relational result

       System.exit(0)
   }

}
