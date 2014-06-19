package bigframe.workflows.runnable

import org.apache.giraph.conf.GiraphConfiguration

trait GiraphRunnable {

	def runGiraph( giraph_config: GiraphConfiguration): Boolean
}