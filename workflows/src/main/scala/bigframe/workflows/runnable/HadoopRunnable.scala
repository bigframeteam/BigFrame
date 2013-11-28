package bigframe.workflows.runnable
import org.apache.hadoop.conf.Configuration

/**
 * Implement this interface such that a query can be run on hadoop.
 * 
 * @author andy
 *
 */
trait HadoopRunnable {

	/**
	 * Run true is the query finish normally.
	 */
	def runHadoop(mapred_config: Configuration = null): java.lang.Boolean
}
