package bigframe.spark.text

import com.codahale.jerkson.Json._

/**
 * User details
 */
case class User(val id_str:String) {
  
//	def this(map: Map[String, Any]) = this(map.get("id") match {case Some(i) => i.toString})
//
//	def apply(map: Map[String, Any]) = {
//		println("In overloaded constructor user")
//		new Tweet(map)
//	}

}

/**
 * Class to represent relevant attributes of a tweet as a tuple.
 * Assumption: The tweet text starts with item_sk (product identifier)
 */
case class Tweet(val text: String, val created_at: String, val user: User, val sentiment: Double = 0.0) {
  @transient lazy val product_id = text.split(" ")(0)

  override def toString: String = {
	return product_id + ", " + text + ", " + created_at + ", " + user.id_str + ", " + sentiment
  }
  
//  def this(map: Map[String, Any]) = { 
//	  val text = map.get("text").toString
//	  val created_at = map.get("created_at").toString
//	  val user = new User(parse[Map[String, Any]](map.get("user")))
//	  this(map.get("text") match {case Some(i) => i.toString}, 
//	      map.get("created_at") match {case Some(i) => i.toString}, 
//	      parse[User](map.get("user") match {case Some(i) => i.toString}))
//  }
//
//  def apply(map: Map[String, Any]) = {
//	  println("In overloaded constructor tweet")
//	  new Tweet(map)
//  }

}

