package bigframe.spark

/*
 Class to represent relevant attributes of a tweet as a tuple.
 Assumption: The tweet text starts with item_sk (product identifier)
*/
case class Tweet(val text: String, val created_at: String, val sentiment: Double = 0.0) {
  @transient lazy val product_id = text.split(" ")(0)

  override def toString: String = {
	return product_id + ", " + text + ", " + created_at + ", " + sentiment
  }
}

