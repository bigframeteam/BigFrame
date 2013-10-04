/**
 *
 */
package bigframe.spark.text

/**
 * @author kunjirm
 *
 */

case class User(val id: Int, name: String) {

}

case class Entities(val hashtags: List[String]) {
	
}

/**
 * Class to represent relevant attributes of a tweet as a tuple.
 * Assumption: The tweet text starts with item_sk (product identifier)
 */
case class Tweet(val text: String, val id: String, val created_at: String, 
    val user: User, val entities: Entities, val score:Double = 0.0) {

	@transient lazy val productID = text.split(" ")(0)

	def products = entities.hashtags
	
	def userID = user.id
	
	def userName = user.name
}
