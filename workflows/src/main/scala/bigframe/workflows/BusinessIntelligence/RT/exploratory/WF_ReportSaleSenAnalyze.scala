/**
 *
 */
package bigframe.workflows.BusinessIntelligence.RT.exploratory

import bigframe.workflows.Query
import bigframe.workflows.runnable.HadoopRunnable
import bigframe.workflows.BaseTablePath

import org.apache.hadoop.conf.Configuration
/**
 * @author andy
 *
 */
class WF_ReportSaleSenAnalyze(basePath : BaseTablePath) extends Query with HadoopRunnable{


	
	def printDescription(): Unit = {}

	override def runHadoop(mapred_config: Configuration): java.lang.Boolean = {
			
		return true
	}
	
}