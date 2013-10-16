package bigframe.workflows

import org.apache.spark.SparkContext
import SparkContext._

abstract class SparkJob(var basePath: BaseTablePath) extends SparkRunnable {	
    protected var sc: SparkContext = _

	def setSparkContext(spark_context: SparkContext) {
        sc = spark_context
    }

 	def run(spark_context: SparkContext): Boolean
}