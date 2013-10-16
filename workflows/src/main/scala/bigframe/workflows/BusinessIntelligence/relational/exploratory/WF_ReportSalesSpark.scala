package bigframe.workflows.BusinessIntelligence.relational.exploratory

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import bigframe.workflows.BaseTablePath
import bigframe.workflows.SparkRunnable

class WF_ReportSalesSpark(basePath : BaseTablePath) extends SparkRunnable {
    final var OUTPUT_PATH = "OUTPUT_PATH"
    private var output_path: String = System.getenv(OUTPUT_PATH) + "/spark/relational"
    private val tpcds_path = basePath.relational_path
    private var sc: SparkContext = _


	/**
	* Selectivity constraints
	* TODO: Find a way to specify them
	*/
	val promotionStart:Int = 1
	val promotionEnd:Int = 100

	private def readFile(name: String) = {
		println("Reading file: " + name)
		sc.textFile(tpcds_path + "/" + name).map(t => t.split('|'))
	}
  
	private def throwException(e: Exception) {
		throw new IllegalStateException("Exiting due to ill-formatted data\n" + e)
	}
  
	/**
	* Select promotions within a list of ids
	*/
	private def applySelectivity(promotion: RDD[Array[String]]) = {
		val selected = promotionStart until promotionEnd
		promotion filter (t => selected contains t(0).toInt)
	}
  
	/**
	* Reads date_dim and returns an RDD of (d_date_sk, date)
	*/
	def dateTuples(): RDD[(String, String)] = {
		try {
			val dates = readFile(ReportSalesConstant.DateTableName)
			dates map {t => (t(0), t(2))}
		} catch {
			case e:Exception => {
			throwException(e)
			sc makeRDD Array(("redundant", "0"))
			}
		}
	}
  
	/**
	* Reads promotion table, applies selectivity parameters.
	* Returns an RDD of (p_item_sk, Array(p_promo_sk, p_promo_id, 
	* p_start_date_sk, p_end_date_sk, p_item_sk))
	*/
	def promotionsMappedByItems(): RDD[(String, Array[String])] = {
		try {
			// Read promotion table, tokenize it, and filter out promotions not in the given list of promo_ids
			val promotion = readFile(ReportSalesConstant.PromotionTableName)

			// Create a mapping from item_sk to promotion tuple
			applySelectivity (promotion) map { t => (t(4), t.slice(0,5)) }
		} catch {
			case e:Exception => {
				throwException(e)
				sc makeRDD Array(("redundant", Array("0")))
			}
		}
	}
  
	/**
	* Given a promotions RDD mapped by p_item_sk and dates RDD (d_date_sk, date),
	* returns promotions RDD with p_start_date_sk and p_end_date_sk replaced with
	* actual dates.
	* @see promotionsMappedByItems()
	* @see dateTuples()
	*/
	def promotionsWithDates(promotions: RDD[(String, Array[String])], 
		dates: RDD[(String, String)]): RDD[(String, Array[String])] = {
		val promoStartDates = promotions.map(t => (t._2(2), t._2)).join(dates)
		.mapValues(t => Array(t._1(0), t._1(1), t._2, t._1(3), t._1(4)))

		val promoDates = promoStartDates.map(t => (t._2(3), t._2)).join(dates)
		.mapValues(t => Array(t._1(0), t._1(1), t._1(2), t._2, t._1(4)))
		.map(t => (t._2(4), t._2))

		promoDates
	}
  
	private def storeSalesPerItem(): RDD[(String, Array[String])] = {
		try {
			// Read sales table and tokenize it
			val sales = readFile(ReportSalesConstant.StoreSalesTableName)

			// Create a mapping from item_sk to sales tuple (sold_date, item_sk, ticket_number, quantity, sales_price)
			sales map { t => (t(2), Array(t(0), t(2), t(9), 
			    try {t(10)} catch { case e: Exception => "0" }, 
			    try {t(13)} catch { case e: Exception => "0" })) }
		} catch {
			case e:Exception => { 
				throwException(e)
				sc makeRDD Array(("redundant", Array("0")))
			}
		}	
	}
  
	private def catalogSalesPerItem(): RDD[(String, Array[String])] = {
		try {
			// Read sales table and tokenize it
			val sales = readFile(ReportSalesConstant.CatalogSalesTableName)

			// Create a mapping from item_sk to sales tuple (sold_date, item_sk, order_number, quantity, sales_price)
			sales map { t => (t(15), Array(t(0), t(15), t(17), 
					try {t(18)} catch { case e: Exception => "0" }, 
					try {t(21)} catch { case e: Exception => "0" })) }
		} catch {
			case e:Exception => {
				throwException(e)
				sc makeRDD Array(("redundant", Array("0")))
			}
		}
	}
  
	private def webSalesPerItem(): RDD[(String, Array[String])] = {
		try {
			// Read sales table and tokenize it
			val sales = readFile(ReportSalesConstant.WebSalesTableName)

			// Create a mapping from item_sk to sales tuple (sold_date, item_sk, order_number, quantity, sales_price)
			sales map { t => (t(3), Array(t(0), t(3), t(17), 
					try{t(18)} catch { case e: Exception => "0" }, 
					try{t(21)} catch { case e: Exception => "0" })) }
		} catch {
			case e:Exception => {
			throwException(e)
			sc makeRDD Array(("redundant", Array("0")))
			}
		}
	}

	/**
	* Joins promotions with supplied sales channel
	* Returns (item_sk, (promotion_id, sales))
	*/
	private def promoJoinSales(promotions: RDD[(String, Array[String])], sales: RDD[(String, Array[String])]) = {
		// join promotions with sales, filter irrelevant attributes, and filter sales not within promotion dates.
		promotions.join(sales).mapValues(t => (t._1(1), t._1(2), t._1(3), t._2(0), 
			  ( try{t._2(3).toDouble*t._2(4).toDouble} catch{case e:Exception => 0} )))
			  .filter(t => (t._2._2 <= t._2._4 & t._2._4 <= t._2._3)).mapValues(t => (t._1, t._5))
	}
  
	/**
	* Relational part of the promotion workflow
	* TODO: Write a SQL version
	*/
	def salesPerPromotion(promotions: RDD[(String, Array[String])]) = {

		// get all sales
		val store_sales = storeSalesPerItem()
		val catalog_sales = catalogSalesPerItem()
		val web_sales = webSalesPerItem()
    
	    // join promotions with sales
	    val promo_store_sales = promoJoinSales(promotions, store_sales)
//    	println("Number of tuples joining store sales: " + promo_store_sales.count())
    	val promo_catalog_sales = promoJoinSales(promotions, catalog_sales)
//   	println("Number of tuples joining catalog sales: " + promo_catalog_sales.count())
   		val promo_web_sales = promoJoinSales(promotions, web_sales)
//    	println("Number of tuples joining web sales: " + promo_web_sales.count())
    
    	// TODO: Watch out order of operations. Which one is most optimized?
  		// TODO: Can we print schedule used by Spark?
    
	    // union three join results
	    val promo_sales = promo_store_sales union promo_catalog_sales union promo_web_sales
//    	println("Number of tuples in union: " + promo_sales.count())

    	// group items together, sum up the sales
    	promo_sales.reduceByKey( (a, b) => (a._1, a._2 + b._2) )
	}
  
	/**
	* Run a relational microbenchmark.
	* TODO: Write SQL for this workflow
	*/
    override def run(spark_context: SparkContext): Boolean = {
    	try {
	    	setSparkContext(spark_context)

			val promotions = promotionsMappedByItems()

			val total_sales = salesPerPromotion(promotions)

			// load item table, tokenize it
			val item = sc.textFile(tpcds_path+"/"+ReportSalesConstant.ItemTableName).map(t => t.split('|')) 

			// map item_sk with product name
			val item_mapped = item map { t => try {(t(0), t(21))} catch { case e:Exception => (t(0), "N/A") } }   

			// join total_sales with item_mapped
			val report = total_sales.join(item_mapped).map { t => (t._2._1._1, t._2._2, t._2._1._2) } 

			// save the output to hdfs
			output_path = output_path + "/spark/relational"
			println("Workflow executed, writing the output to: " + output_path)
	        report.saveAsTextFile(output_path)
    	} catch{
    		case ex: Exception => {
            	return false
         	}
    	}
		return true
  	}

  	def setSparkContext(spark_context: SparkContext) {
        sc = spark_context
    }

  
}