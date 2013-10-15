package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException

import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

/**
 * A Mapper class for getting the sales of a set of items.
 * 
 *  @author andy
 */
class ReportSalesMapper extends Mapper[LongWritable, Text, Text, FloatWritable] {

	type Context = Mapper[LongWritable, Text, Text, FloatWritable]#Context
	/**
	 * Do a map-side join to get sales for a set of items.  
	 */
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	override def map(key: LongWritable,
			value: Text, context: Context) = {
		val mapreduce_config = context.getConfiguration()
		val uris = DistributedCache.getLocalCacheFiles(mapreduce_config)

		var promotIDs = List[String]()
		var itemSKs = List[String]()
  	   	var dateBeginSKs = List[Int]()
  	   	var dateEndSKs = List[Int]()
  	   
  	   	for(i <-  uris.indices) {
  	   		val in = new BufferedReader(new FileReader(uris(i).toString()))
  		   
  	   		if (uris(i).toString().contains("promotion")) {
  	   			var line = in.readLine()
  	   			while (line != null) {
  	   				val fields = line.split("\\|", -1)

    				val (promtID, datebegSK, dateendSK, prodSK) = 
    					(fields(1), fields(2), fields(3), fields(4))

    				if (datebegSK != "" && dateendSK != ""
    						&& prodSK != "" && promtID != "") {
    					promotIDs ::= promtID
    					itemSKs ::= prodSK
    					dateBeginSKs ::= datebegSK.toInt
    					dateEndSKs ::= dateendSK.toInt
  					   
    				}
    				line = in.readLine()
  	   			}
  	   		}
  	   	}
  	   
		val fileSplit: FileSplit = context.getInputSplit().asInstanceOf[FileSplit]
		val filename = fileSplit.getPath().getName()
		
		val fields = value.toString().split("\\|", -1)
		
		var (sold_dateSK, item_sk, quantity, price) = {
			if (filename.contains("store_sales") && 
					fields(0)!="" && fields(2)!="" && fields(10)!="" && fields(13)!="")
				(fields(0).toInt, fields(2), fields(10).toInt, fields(13).toFloat)
			else if(filename.contains("web_sales") &&
					fields(0)!="" && fields(3)!="" && fields(18)!="" && fields(21)!="")
				(fields(0).toInt, fields(3), fields(18).toInt, fields(21).toFloat)
			else if(filename.contains("catalog_sales") &&
				fields(0)!="" && fields(15)!="" && fields(18)!="" && fields(21)!="")		
				(fields(0).toInt, fields(15), fields(18).toInt, fields(21).toFloat)
			else 
				(0, "", 0, 0f)
		}
		/**
		 * Group the sales by (promotionID, itemSK)
		 */
		for(i <- itemSKs.indices) {
			if(item_sk == itemSKs(i) && dateBeginSKs(i) <= sold_dateSK && 
				sold_dateSK <= dateEndSKs(i)) {
				context.write(new Text(promotIDs(i) +"\\|" +itemSKs(i)), new FloatWritable(quantity * price))
			}
		}
				
	}	
  	

}