package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.IOException;


import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

/**
 * A Reducer class to aggregate the sales by (promotionID, itemSK).
 * 
 * @author andy
 */
class ReportSalesReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] {

	type Context = Reducer[Text, FloatWritable, Text, FloatWritable]#Context
	
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	override def reduce(key: Text,
			values: java.lang.Iterable[FloatWritable], context: Context) = {
		var mapreduce_config = context.getConfiguration()
		mapreduce_config.set("mapreduce.output.key.field.separator", "\\|")
		
		var total_sales = 0f
		values.foreach(value => total_sales += value.get)
		
		context.write(key, new FloatWritable(total_sales))
	}
}