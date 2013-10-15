package bigframe.workflows

/**
 * A class stores the path for each specific data type.
 *  
 * @author andy
 *
 */
class BaseTablePath {
	
	private var _relationalpath = ""
	private var _graphpath = ""
	private var _nestedpath = ""
	
	/**
	 * Getters
	 */
	def relational_path = _relationalpath
	
	def graph_path = _graphpath
	
	def nested_path = _nestedpath
	
	
	/**
	 * Setters
	 */
	def relational_path_= (value: String):Unit = _relationalpath = value
	
	def graph_path_= (value: String):Unit = _graphpath = value
	
	def nested_path_= (value: String):Unit = _nestedpath = value
	
}
