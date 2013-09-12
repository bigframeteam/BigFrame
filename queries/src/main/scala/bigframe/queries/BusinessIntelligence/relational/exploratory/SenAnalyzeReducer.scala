package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.IOException

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

/**
 * Reducer class for aggregating sentiment score by product name.
 * 
 * @author andy
 */
class SenAnalyzeReducer extends Reducer[Text, FloatWritable, Text, FloatWritable] {

	type Context = Reducer[Text, FloatWritable, Text, FloatWritable]#Context
	
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	override def reduce(key: Text,
			values: java.lang.Iterable[FloatWritable], context: Context) = {
		
		var total_sentiment = 0f
		values.foreach(value => total_sentiment += value.get)
		
		context.write(key, new FloatWritable(total_sentiment))
	}
}