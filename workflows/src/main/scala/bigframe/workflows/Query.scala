package bigframe.workflows


import scala.collection.mutable.Set

/**
 * The class provide basic information of a query.
 * 
 * @author andy
 *
 */
abstract class Query {
	protected var _dataTypes : Set[String] = Set()
	protected var _queryType = ""
	protected var _description = ""
	protected var _runningEngine = ""
	
	/**
	 * Getters
	 */
	def dataTypes = _dataTypes
	
	def queryType = _queryType
	
	def description = _description
	
	def runningEngine = _runningEngine
	
	/**
	 * Setters
	 */
	def dataTypes_= (value : Set[String]): Unit = _dataTypes = value
	
	def queryType_= (value : String): Unit = _queryType = value
	
	def description_= (value : String): Unit = _description = value
	
	def runningEngine_= (value : String): Unit = _runningEngine = value
	
	/**
	 * Print out the description of this query
	 */
	def printDescription(): Unit
	
}
