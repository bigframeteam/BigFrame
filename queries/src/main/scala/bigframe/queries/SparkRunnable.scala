package bigframe.queries

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

/**
 * Implement this interface such that a query can be run on spark.
 * 
 * @author andy
 */
trait SparkRunnable {

  def run(spark_context: SparkContext): RDD[_]
}	