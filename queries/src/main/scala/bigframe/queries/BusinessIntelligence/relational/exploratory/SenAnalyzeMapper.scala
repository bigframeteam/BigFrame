package bigframe.queries.BusinessIntelligence.relational.exploratory

import java.io.IOException

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.Text

import com.codahale.jerkson.Json._

/**
 * Mapper class for extracting sentiment score.
 * 
 * @author andy
 */
class SenAnalyzeMapper extends Mapper[LongWritable, Text , Text, FloatWritable] {
	
	type Context = Mapper[LongWritable, Text, Text, FloatWritable]#Context
	/**
	 * map function for extracting sentiment score.
	 */
	@throws(classOf[IOException])
	@throws(classOf[InterruptedException])
	override def map(key: LongWritable,
			value: Text, context: Context) = {
		
		val simplejson = parse[Map[String, Any]](value.toString())
	
	}
}