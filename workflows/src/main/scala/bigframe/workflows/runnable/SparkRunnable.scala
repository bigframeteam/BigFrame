
package bigframe.workflows.runnable

import spark.SparkContext
import spark.SparkContext._

/**
 * Implement this interface such that a query can be run on spark.
 * 
 * @author andy
 */
trait SparkRunnable {

  def runSpark(sc: SparkContext): Boolean
  
}


