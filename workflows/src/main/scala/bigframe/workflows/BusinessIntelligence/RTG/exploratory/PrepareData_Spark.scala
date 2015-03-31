package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}

import com.google.common.io.Files

import java.io.File
import java.lang.Iterable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import SparkContext._

import org.apache.hadoop.mapreduce.Job
import bigframe.avro.Promotion
import bigframe.avro.SerializablePromotion
import bigframe.avro.Tweet
import bigframe.avro.SerializableTweet
import bigframe.avro.Url
import bigframe.avro.User_Mention
import bigframe.avro.User
import bigframe.avro.Entities
import bigframe.workflows.BaseTablePath

import bigframe.util.parser.JsonParser

import org.json.simple.JSONArray
import org.json.simple.JSONObject

object PrepareData_Spark extends Serializable {
	val promo_dir = "promo_avro"
//	val tempDir = Files.createTempDir()
//	val promo_dir = new File(tempDir, "output").getAbsolutePath
//	println(promo_dir)
	val tweet_dir = "tweet_avro"
}

class PrepareData_Spark(basePath: BaseTablePath) extends Serializable {

	val itemHDFSPath = basePath.relational_path + "/item"
	val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	val promotionHDFSPath = basePath.relational_path + "/promotion"
	val customerHDFSPath =  basePath.relational_path + "/customer"
	val date_dimHDFSPath = basePath.relational_path + "/date_dim"
	
	val twitter_grapgHDFSPath = basePath.graph_path
	val tweets_HDFSPath = basePath.nested_path	
	
	def getPromotion(sc: SparkContext, path: String) = {
		
		val promo_list = sc.textFile(path).map( line => {
			val promo = new Promotion
			val fields = line.split("\\|", -1)
			
			promo.setPPromoSk(fields(0).toInt)
			promo.setPPromoId(fields(1))
			promo.setPStartDateSk({if(!fields(2).isEmpty()) fields(2).toInt else null})
			promo.setPEndDateSk({if(!fields(3).isEmpty()) fields(3).toInt else null})
			promo.setPItemSk({if(!fields(4).isEmpty()) fields(4).toInt else null})
			promo.setPCost({if(!fields(5).isEmpty()) fields(5).toFloat else null})
			promo.setPResponseTarget({if(!fields(6).isEmpty()) fields(6).toInt else null})
			promo.setPPromoName(fields(7))
			promo.setPChannelDmail(fields(8))
			promo.setPChannelEmail(fields(9))
			promo.setPChannelCatalog(fields(10))
			promo.setPChannelTv(fields(11))
			promo.setPChannelRadio(fields(12))
			promo.setPChannelPress(fields(13))
			promo.setPChannelEvent(fields(14))
			promo.setPChannelDemo(fields(15))
			promo.setPChannelDetails(fields(16))
			promo.setPPurpose(fields(17))
			promo.setPDiscountActive(fields(18))
			
			promo
		})
		
		promo_list
	}
	
	def getTweet(sc: SparkContext, path: String) = {
		
		val tweet_list = sc.textFile(path).map( line => {
			val tweet = new Tweet
			val tweet_json = JsonParser.parseJsonFromString(line.toString())
			
			val entities = new Entities
			val entities_json = tweet_json.get("entities").asInstanceOf[JSONObject]
			
			val urls_json = entities_json.get("urls").asInstanceOf[JSONArray]
			
			val urls = new java.util.ArrayList[Url]
			
			for( i <- 0 until urls_json.size()) {
				val url = new Url
				
				val url_json = urls_json.get(i).asInstanceOf[JSONObject]
				
				url.setExpandedUrl(url_json.get("expanded_url").asInstanceOf[String])
				url.setUrl(url_json.get("url").asInstanceOf[String])
				url.setIndices(url_json.get("indices").asInstanceOf[java.util.List[Integer]])
				
				urls.add(url)
			}
			
			val hashtags = entities_json.get("hashtags").asInstanceOf[java.util.List[java.lang.CharSequence]]
			
			val user_mentions_json = entities_json.get("user_mentions").asInstanceOf[JSONArray]
			val user_mentions = new java.util.ArrayList[User_Mention]
			
			for( i <- 0 until user_mentions_json.size()) {
				val user_mention = new User_Mention
				
				val user_mention_json = user_mentions_json.get(i).asInstanceOf[JSONObject]
				
				user_mention.setId(user_mention_json.get("id").asInstanceOf[Long])
				user_mention.setIdStr(user_mention_json.get("id_str").asInstanceOf[String])
				user_mention.setName(user_mention_json.get("name").asInstanceOf[String])
				user_mention.setScreenName(user_mention_json.get("screen_name").asInstanceOf[String])
				user_mention.setIndices(user_mention_json.get("indices").asInstanceOf[java.util.List[Integer]])
				
				user_mentions.add(user_mention)
			}
			
			entities.setHashtags(hashtags)
			entities.setUrls(urls)
			entities.setUserMentions(user_mentions)
			
			val user = new User
			val user_json = tweet_json.get("user").asInstanceOf[JSONObject]
			
			user.setContributorsEnabled(user_json.get("contributors_enabled").asInstanceOf[String])
			user.setCreatedAt(user_json.get("created_at").asInstanceOf[String])
			user.setDescription(user_json.get("description").asInstanceOf[String])
			user.setFavouritesCount(user_json.get("favourites_count").asInstanceOf[String])
			user.setFollowersCount(user_json.get("followers_count").asInstanceOf[String])
			user.setFollowing(user_json.get("following").asInstanceOf[String])
			user.setFollowRequestSent(user_json.get("follow_request_sent").asInstanceOf[String])
			user.setFriendsCount(user_json.get("friends_count").asInstanceOf[String])
			user.setGeoEnabled(user_json.get("geo_enabled").asInstanceOf[String])
			user.setId(user_json.get("id").asInstanceOf[Long])
			user.setIdStr(user_json.get("id_str").asInstanceOf[String])
			user.setLang(user_json.get("lang").asInstanceOf[String])
			user.setListedCount(user_json.get("listed_count").asInstanceOf[String])
			user.setLocation(user_json.get("location").asInstanceOf[String])
			user.setName(user_json.get("name").asInstanceOf[String])
			user.setNotifications(user_json.get("notifications").asInstanceOf[String])
			user.setProfileBackgroundColor(user_json.get("profile_background_color").asInstanceOf[String])
			user.setProfileBackgroundImageUrl(user_json.get("profile_background_image_url").asInstanceOf[String])
			user.setProfileBackgroundTile(user_json.get("profile_background_tile").asInstanceOf[String])
			user.setProfileImageUrl(user_json.get("profile_image_url").asInstanceOf[String])
			user.setShowAllInlineMedia(user_json.get("show_all_inline_media").asInstanceOf[String])
			user.setProfileLinkColor(user_json.get("profile_link_color").asInstanceOf[String])
			user.setProfileSidebarBorderColor(user_json.get("profile_sidebar_border_color").asInstanceOf[String])
			user.setProfileSidebarFillColor(user_json.get("profile_sidebar_fill_color").asInstanceOf[String])
			user.setProfileTextColor(user_json.get("profile_text_color").asInstanceOf[String])
			user.setVerified(user_json.get("verified").asInstanceOf[String])
			user.setUtcOffset(user_json.get("utc_offset").asInstanceOf[String])
			user.setUrl(user_json.get("url").asInstanceOf[String])
			user.setTimeZone(user_json.get("time_zone").asInstanceOf[String])
			user.setStatusesCount(user_json.get("statuses_count").asInstanceOf[String])
			user.setScreenName(user_json.get("screen_name").asInstanceOf[String])
			user.setProfileUseBackgroundImage(user_json.get("profile_use_background_image").asInstanceOf[String])
			user.setProtected$(user_json.get("protected").asInstanceOf[String])
			
			tweet.setContributors(tweet_json.get("contributors").asInstanceOf[String])
			tweet.setCoordinates(tweet_json.get("coordinates").asInstanceOf[String])
			tweet.setCreatedAt(tweet_json.get("created_at").asInstanceOf[String])
			tweet.setEntities(entities)
			tweet.setFavorited(tweet_json.get("favorited").asInstanceOf[String])
			tweet.setGeo(tweet_json.get("geo").asInstanceOf[String])
			tweet.setId(tweet_json.get("id").asInstanceOf[Long])
			tweet.setIdStr(tweet_json.get("id_str").asInstanceOf[String])
			tweet.setInReplyToScreenName(tweet_json.get("in_reply_to_screen_name").asInstanceOf[String])
			tweet.setInReplyToStatusId(tweet_json.get("in_reply_to_status_id").asInstanceOf[String])
			tweet.setInReplyToStatusIdStr(tweet_json.get("in_reply_to_status_id_str").asInstanceOf[String])
			tweet.setInReplyToUserId(tweet_json.get("in_reply_to_user_id").asInstanceOf[String])
			tweet.setInReplyToUserIdStr(tweet_json.get("in_reply_to_user_id_str").asInstanceOf[String])
			tweet.setPlace(tweet_json.get("place").asInstanceOf[String])
			tweet.setUser(user)
			tweet.setTruncated(tweet_json.get("truncated").asInstanceOf[String])
			tweet.setText(tweet_json.get("text").asInstanceOf[String])
			tweet.setSource(tweet_json.get("source").asInstanceOf[String])
			tweet.setRetweeted(tweet_json.get("retweeted").asInstanceOf[String])
			tweet.setRetweetCount(tweet_json.get("retweet_count").asInstanceOf[String])
			
			tweet
		})
		
		tweet_list
	}
	
	def writePromotion(sc: SparkContext, promo_list: RDD[Promotion]) = {
		val job = new Job()
		// Configure the ParquetOutputFormat to use Avro as the serialization format
		ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
		
		// You need to pass the schema to AvroParquet when you are writing objects but not when you
		// are reading them. The schema is saved in Parquet file for future readers to use.
		AvroParquetOutputFormat.setSchema(job, Promotion.SCHEMA$)
		
		// Create a PairRDD with all keys set to null and wrap each promotion acid in serializable objects
		val promo_rdd = (promo_list.map(promo => (null,  new SerializablePromotion{setValues(promo)})))
//		val promo_rdd = promo_list.map(promo => (null,  promo))

		// Save the RDD to a Parquet file in our temporary output directory
		promo_rdd.saveAsNewAPIHadoopFile(PrepareData_Spark.promo_dir, classOf[Void], classOf[SerializablePromotion],
				classOf[ParquetOutputFormat[SerializablePromotion]], job.getConfiguration)
	}
	
	def writeTweet(sc: SparkContext, tweet_list: RDD[Tweet]) = {
		val job = new Job()
						
		ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
		AvroParquetOutputFormat.setSchema(job, Tweet.SCHEMA$)
		val tweet_rdd =	tweet_list.map(tweet => (null, new SerializableTweet(tweet))) 
		
		tweet_rdd.saveAsNewAPIHadoopFile(PrepareData_Spark.tweet_dir, classOf[Void], classOf[Tweet],
				classOf[ParquetOutputFormat[Tweet]], job.getConfiguration)
	}
	
	def readPromotion(sc: SparkContext) = {
		val job = new Job()
		
		ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Promotion]])
		val file = sc.newAPIHadoopFile(PrepareData_Spark.promo_dir, classOf[ParquetInputFormat[Promotion]],
				classOf[Void], classOf[Promotion], job.getConfiguration)
				
		file.map(t => t._2)
	}
	
	def readTweet(sc: SparkContext): RDD[Tweet] = {
		val job = new Job()
		
		ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Tweet]])
		val file = sc.newAPIHadoopFile(PrepareData_Spark.tweet_dir, classOf[ParquetInputFormat[Tweet]],
				classOf[Void], classOf[Tweet], job.getConfiguration)
		
		file.map(t => t._2)
		
	}
		
	def prepareData(sc: SparkContext) = {
		
	}
		
}