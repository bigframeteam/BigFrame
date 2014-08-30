/**
 *
 */
package bigframe.workflows.runnable

import java.sql.Connection
import org.apache.spark.sql.hive.HiveContext
import bigframe.workflows.events.BigFrameListenerBus

/**
 * @author mayuresh
 *
 */
trait MixedRunnable {

/*
* Prepeare the basic tables before run the Hive query
*/
def prepareHiveTables(hc:HiveContext, connection:Connection): Unit

/**
* Run the benchmark query
*/
def runMixedFlow(hc:HiveContext, connection:Connection,
    eventBus:BigFrameListenerBus): Boolean

def cleanUp(hc:HiveContext, connection: Connection): Unit
}