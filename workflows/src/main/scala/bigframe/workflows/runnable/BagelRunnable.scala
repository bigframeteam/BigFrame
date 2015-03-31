package bigframe.workflows.runnable

import org.apache.spark.SparkContext
import SparkContext._

trait BagelRunnable {
  def runBagel(sc: SparkContext): Boolean
}