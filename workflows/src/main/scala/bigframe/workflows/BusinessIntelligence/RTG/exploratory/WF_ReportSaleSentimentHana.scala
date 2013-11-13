package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future
import java.sql.Connection
import java.sql.SQLException

import bigframe.workflows.Query
import bigframe.workflows.runnable.HanaRunnable
import bigframe.workflows.BaseTablePath

import scala.collection.JavaConversions._

class WF_ReportSaleSentimentHana(basePath: BaseTablePath, num_iter: Int) extends Query with HanaRunnable {

    final val SCHEMA : String = "BIGFRAME"
	
    def printDescription(): Unit = {}


	override def prepareHanaTables(connection: Connection): Unit = {}
	
	override def runHana(connection: Connection): Boolean = {
			val stmt = connection.createStatement()		
		try {

			//val set_schema = "SET SCHEMA \"" + SCHEMA + "\""
			//stmt.execute(set_schema);

			val databaseMetaData2 = connection.getMetaData()

			val add_dropTableIfExists = "CREATE PROCEDURE DROP_TABLE_IF_EXISTS " +
					"( IN tablename VARCHAR(20),IN schemaname varchar(20) " +
					") LANGUAGE SQLSCRIPT AS myrowid integer; " +
					"BEGIN " +
					"myrowid := 0; " +
					"SELECT COUNT(*) INTO myrowid FROM \"PUBLIC\".\"M_TABLES\"" +
					"WHERE schema_name =:schemaname AND table_name=:tablename;" +
					"IF (:myrowid > 0 ) THEN " +
					"exec 'DROP TABLE '||:schemaname||'.'||:tablename; " +
					"END IF; " +
					"END; "

			try{
				val drop_dropTableIfExists = "DROP PROCEDURE DROP_TABLE_IF_EXISTS"
				stmt.execute(drop_dropTableIfExists)
				stmt.execute(add_dropTableIfExists);
			} 
			catch {
				case se :
					SQLException => stmt.execute(add_dropTableIfExists);
			}

			
			val add_dropProcedureIfExists = "CREATE PROCEDURE DROP_PROCEDURE_IF_EXISTS " +
					"( IN procname VARCHAR(20),IN schemaname varchar(20) " +
					") LANGUAGE SQLSCRIPT AS myrowid integer; " +
					"BEGIN " +
					"myrowid := 0; " +
					"SELECT COUNT(*) INTO myrowid FROM \"PUBLIC\".\"PROCEDURES\" " +
					"WHERE schema_name = :schemaname AND procedure_name = :procname; " +
					"IF (:myrowid > 0 ) THEN " +
					"exec 'DROP PROCEDURE '||:schemaname||'.'||:procname; " +
					"END IF; " +
					"END; "

			try{
				val drop_dropProcedureIfExists = "DROP PROCEDURE DROP_PROCEDURE_IF_EXISTS"
				stmt.execute(drop_dropProcedureIfExists)
				stmt.execute(add_dropProcedureIfExists);
			} 
			catch {
				case se :
					SQLException => stmt.execute(add_dropProcedureIfExists);
			}		
			
			val drop_promotionSelected = "CALL DROP_TABLE_IF_EXISTS(\'PROMOTIONSELECTED\', \'" + SCHEMA + "\')"
			val create_promotionSelected = "CREATE COLUMN TABLE promotionSelected (promo_id char(16), item_sk int," +
					"start_date_sk int, end_date_sk int)"
			stmt.execute(drop_promotionSelected)
			stmt.execute(create_promotionSelected)
			
			val promotSKs = "1"
			val lower = 1
			val upper = 100
			val query_promotionSelected = "INSERT INTO promotionSelected " + 
					"	SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk " +
					"	FROM promotion " +
//					"	WHERE p_promo_sk in (" + promotSKs + ")" 
					"	WHERE " + lower + " < p_promo_sk AND p_promo_sk < " + upper 
			

			stmt.executeUpdate(query_promotionSelected)

			val drop_promotedProduct = "CALL DROP_TABLE_IF_EXISTS(\'PROMOTEDPRODUCT\', \'" + SCHEMA + "\')"
			val create_promotedProduct = "CREATE COLUMN TABLE promotedProduct (item_sk int, product_name char(50))" 
			stmt.execute(drop_promotedProduct)
			stmt.execute(create_promotedProduct)	
				
			val query_pomotedProduct = "INSERT INTO promotedProduct " +
					"	SELECT i_item_sk, i_product_name " +
					"	FROM item JOIN promotionSelected " +
					"	ON i_item_sk = item_sk "
						
			stmt.executeUpdate(query_pomotedProduct)
			
			
			val drop_RptSalesByProdCmpn = "CALL DROP_TABLE_IF_EXISTS(\'RPTSALESBYPRODCMPN\', \'" + SCHEMA + "\')"
			val create_RptSalesByProdCmpn = "CREATE COLUMN TABLE RptSalesByProdCmpn " +
					"	(promo_id char(16), item_sk int, totalsales float)"
			stmt.execute(drop_RptSalesByProdCmpn)
			stmt.execute(create_RptSalesByProdCmpn)
						
			val query_RptSalesByProdCmpn = "INSERT INTO RptSalesByProdCmpn " +
					"		SELECT p.promo_id, p.item_sk, sum(price*quantity) as totalsales " +
					"		FROM" + 
					"			(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
					"			FROM web_sales " + 
					"			UNION ALL " + 
					"			SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity "  +
					"			FROM store_sales" + 
					"			UNION ALL" + 
					"			SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
					"			FROM catalog_sales) sales"  +
					"			JOIN promotionSelected p "  +
					"			ON sales.item_sk = p.item_sk" +
					"		WHERE " + 
					"			p.start_date_sk <= sold_date_sk " +
					"			AND" + 
					"			sold_date_sk <= p.end_date_sk" +
					"		GROUP BY" + 
					"			p.promo_id, p.item_sk "
							
			stmt.executeUpdate(query_RptSalesByProdCmpn)
			
			val drop_relevantTweet = "CALL DROP_TABLE_IF_EXISTS(\'RELEVANTTWEET\', \'" + SCHEMA + "\')"
			val create_relevantTweet = "CREATE COLUMN TABLE relevantTweet" +
					"	(item_sk int, user_id int, text varchar(200), senti_score int )"

			stmt.execute(drop_relevantTweet)
			stmt.execute(create_relevantTweet)
			
			val query_relevantTweet = "INSERT INTO relevantTweet" +
					"	SELECT item_sk, user_id, text, null" +
					"	FROM " +
					"		(SELECT item_sk, tweet_id" +
					"		FROM promotedProduct " +
					"		JOIN entities" +
					"		ON product_name = hashtag) t" +
					"	JOIN tweet " +
					"	ON tweet_id = id" 
					
			stmt.executeUpdate(query_relevantTweet)

			val drop_senAnalyse = "CALL DROP_TABLE_IF_EXISTS(\'SENANALYSE\', \'" + SCHEMA + "\')"
			val create_senAnalyse = "CREATE COLUMN TABLE senAnalyse" +
					"	(item_sk int, user_id int, sentiment_score int)"
			
			stmt.execute(drop_senAnalyse)
			stmt.execute(create_senAnalyse)
			
			val drop__senAnalyze_procedure = "CALL DROP_PROCEDURE_IF_EXISTS(\'SENTIMENT_ANALYZE\', \'" + SCHEMA + "\')"
			val create_senAnalyze_procedure = "create procedure sentiment_analyze language SQLSCRIPT AS " + 
					"stop_pos int := 1; " + 
					"score int := 0;   " + 
					"score_tmp int; " + 
					"score_tmp2 int; " + 
					"text nvarchar(200) := '';  " + 
					"text_tmp nvarchar(200) := ''; " + 
					"word_tmp nvarchar(200) := ''; " + 
					"cursor text_cursor for " + 
					"select text from relevantTweet; " + 
					"CURSOR score_cursor (word_key VARCHAR(200)) FOR " + 
					"SELECT score FROM score_map " + 
					"WHERE name = :word_key; " + 
					"BEGIN    " + 
					"	FOR row_tmp as text_cursor DO " + 
					"	  text := row_tmp.text; " + 
					"	  stop_pos := 1; " + 
					"	  score := 0; " + 
					"	  while :stop_pos = 1 do " + 
					"	    word_tmp :=  SUBSTR_BEFORE (:text,' '); " + 
					"	    text_tmp := SUBSTR_AFTER (:text,' '); " + 
					"	    if length(:word_tmp) = 0 then " + 
					"	      stop_pos := 0; " + 
					"	    else " + 
					"	      text := substr_after(:text, ' '); " + 
					"	      open score_cursor(:word_tmp); " + 
					"	      FETCH score_cursor INTO score_tmp2; " + 
					"	      score_tmp := ifnull(:score_tmp2, 0); " + 
					"	      score := :score + :score_tmp; " + 
					"	      close score_cursor; " + 
					"	    end if;    " + 
					"	  end while; " + 
					"	   " + 
					"	  open score_cursor(:text); " + 
					"	  FETCH score_cursor INTO score_tmp2; " + 
					"	  close score_cursor;  " + 
					"	   " + 
					"	  score_tmp := ifnull(:score_tmp2, 0); " + 
					"	  score := :score + :score_tmp; " + 
					"	   " + 
					"	  UPDATE relevantTweet set senti_score = :score where text = row_tmp.text; " + 
					"	END FOR;   " + 
					"END"
					
			stmt.execute(drop__senAnalyze_procedure)
			stmt.execute(create_senAnalyze_procedure)

			val procedure_senAnalyse = "CALL sentiment_analyze";
			val query_senAnalyse = "INSERT INTO senAnalyse" +
					"	SELECT item_sk, user_id, sum(senti_score) as sum_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
					
			stmt.execute(procedure_senAnalyse)
			stmt.executeUpdate(query_senAnalyse)

			val drop_tweetByUser = "CALL DROP_TABLE_IF_EXISTS(\'TWEETBYUSER\', \'" + SCHEMA + "\')"
			val create_tweetByUser = "CREATE COLUMN TABLE tweetByUser (user_id int, num_tweets int)"
			
			stmt.execute(drop_tweetByUser)
			stmt.execute(create_tweetByUser)
			
			val query_tweetByUser = "INSERT INTO tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			stmt.executeUpdate(query_tweetByUser)
					
			val drop_tweetByProd =  "CALL DROP_TABLE_IF_EXISTS(\'TWEETBYPROD\', \'" + SCHEMA + "\')"
			val create_tweetByProd = "CREATE COLUMN TABLE tweetByProd (item_sk int, num_tweets int)"
																																																																																																																		
			stmt.execute(drop_tweetByProd)
			stmt.execute(create_tweetByProd)
			
			val query_tweetByProd = "INSERT INTO tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
			stmt.executeUpdate(query_tweetByProd)
			

			
								
			val drop_sumFriendTweets = "CALL DROP_TABLE_IF_EXISTS(\'SUMFRIENDTWEETS\', \'" + SCHEMA + "\')"
			val create_sumFriendTweets = "CREATE COLUMN TABLE sumFriendTweets (follower_id int, num_friend_tweets int)"
				
			stmt.execute(drop_sumFriendTweets)
			stmt.execute(create_sumFriendTweets)
			
			val query_sumFriendTweets = "INSERT INTO sumFriendTweets" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser JOIN twitter_graph" +
					"	 		ON user_id = friend_id) f" +
					"		ON user_id = follower_id" +
					"		GROUP BY " +
					"		user_id) result"
			
			stmt.executeUpdate(query_sumFriendTweets)
			
			val drop_mentionProb = "CALL DROP_TABLE_IF_EXISTS(\'MENTIONPROB\', \'" + SCHEMA + "\')"
			val create_mentionProb = "CREATE COLUMN TABLE mentionProb (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_mentionProb)
			stmt.execute(create_mentionProb)
			
			val query_mentionProb = "INSERT INTO mentionProb" +
					"	SELECT item_sk, t.user_id, r.num_tweets/t.num_tweets" +
					"	FROM tweetByUser as t JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON t.user_id = r.user_id"
			
			stmt.executeUpdate(query_mentionProb)
			
			val drop_simUserByProd =  "CALL DROP_TABLE_IF_EXISTS(\'SIMUSERBYPROD\', \'" + SCHEMA + "\')"
			val create_simUserByProd = "CREATE COLUMN TABLE simUserByProd " +
					"	(item_sk int, follower_id int, friend_id int, similarity float)"
			
			stmt.execute(drop_simUserByProd)
			stmt.execute(create_simUserByProd)
			
			val query_simUserByProd = "INSERT INTO simUserByProd" +
					"	SELECT f.item_sk, follower_id, friend_id, 1 - ABS(follower_prob - prob)" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON user_id = follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	friend_id = user_id" +
					"	UNION ALL" +
					"	SELECT item_sk, user_id, user_id, 0" +
					"	FROM mentionProb"
					
			stmt.executeUpdate(query_simUserByProd)
					
					
			val drop_transitMatrix = "CALL DROP_TABLE_IF_EXISTS(\'TRANSITMATRIX\', \'" + SCHEMA + "\')"
			val create_transitMatrix = "CREATE COLUMN TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob float)"
				
			stmt.execute(drop_transitMatrix)
			stmt.execute(create_transitMatrix)
			
			val query_transitMatrix = "INSERT INTO transitMatrix" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser" +
					"	ON friend_id = user_id"
					
					
			stmt.executeUpdate(query_transitMatrix)
			
			
			val drop_randSufferVec = "CALL DROP_TABLE_IF_EXISTS(\'RANDSUFFVEC\', \'" + SCHEMA + "\')"
			val create_randSuffVec = "CREATE COLUMN TABLE randSUffVec (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_randSufferVec)
			stmt.execute(create_randSuffVec)
			
			val query_randSuffVec = "INSERT INTO randSuffVec" +
					"	SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd t2" +
					"	ON t1.item_sk = t2.item_sk"
			
			stmt.executeUpdate(query_randSuffVec)
			
			val drop_initalRank =  "CALL DROP_TABLE_IF_EXISTS(\'INITIALRANK\', \'" + SCHEMA + "\')"
			val create_initialRank = "CREATE COLUMN TABLE initialRank (item_sk int, user_id int, rank_score float)"
				
			stmt.execute(drop_initalRank)
			stmt.execute(create_initialRank)
			
			val query_initialRank = "INSERT INTO initialRank" +
					"	SELECT t1.item_sk, user_id, 1/num_users" +
					"	FROM " +
					"		(SELECT item_sk, COUNT(DISTINCT user_id) as num_users" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk) t1 " +
					"	JOIN" +
					"		(SELECT DISTINCT *" +
					"		FROM" +
					"			(SELECT item_sk, user_id" +
					"			FROM relevantTweet) t2" +
					"		) t3" +
					"	ON t1.item_sk = t3.item_sk"
			stmt.executeUpdate(query_initialRank)
					
			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				val drop_twitterRank =  "CALL DROP_TABLE_IF_EXISTS(\'TWITTERRANK" + iteration + "\', \'" + SCHEMA + "\')"
				val create_twitterRank = "CREATE COLUMN TABLE twitterRank"+iteration+" (item_sk int, user_id int, rank_score float)"
			
				stmt.execute(drop_twitterRank)
				stmt.execute(create_twitterRank)
					
				val twitterRank_last = if(iteration == 1) "initialRank" else "twitterRank"+(iteration-1)				

				val query_twitterRank = "INSERT INTO twitterRank"+iteration +
						"	SELECT t4.item_sk, t4.user_id, " +
						"		CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1-alpha) +" * prob" +
						"			 ELSE " + (1-alpha) +" * prob" +
						"		END" +
						"	FROM" +
						"		(SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
						"		FROM transitMatrix t1, " + twitterRank_last +" t2" +
						"		WHERE t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
						"			AND t1.follower_id != t2.user_id" +
						"		GROUP BY " +
						"		t1.item_sk, follower_id) t3" +
						"	RIGHT JOIN randSUffVec t4" +
						"	ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"
			
				stmt.executeUpdate(query_twitterRank)
				
			}
			
			val drop_RptSAProdCmpn = "CALL DROP_TABLE_IF_EXISTS(\'RPTSAPRODCMPN\', \'" + SCHEMA + "\')"
			val create_RptSAProdCmpn = "CREATE COLUMN TABLE RptSAProdCmpn (promo_id char(16), item_sk int, totalsales float , total_sentiment float)"
					
			stmt.execute(drop_RptSAProdCmpn)
			stmt.execute(create_RptSAProdCmpn)
			
			val query_RptSAProdCmpn = "INSERT INTO RptSAProdCmpn" +
					"	SELECT promo_id, t4.item_sk, totalsales, total_sentiment" +
					"	FROM " +
					"		(SELECT t1.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
					"		FROM senAnalyse t1, twitterRank" + num_iter + " t2" +
					"		WHERE t1.item_sk = t2.item_sk AND t1.user_id = t2.user_id" +
					"		GROUP BY" +
					"		t1.item_sk) t3" +
					"	JOIN RptSalesByProdCmpn t4 " +
					"	ON t3.item_sk = t4.item_sk"				

			if (stmt.executeUpdate(query_RptSAProdCmpn) > 0) return true else return false
		} catch {
			case sqle :
				SQLException => sqle.printStackTrace()
			case e :
				Exception => e.printStackTrace()
		}

		
		return false
	}
}