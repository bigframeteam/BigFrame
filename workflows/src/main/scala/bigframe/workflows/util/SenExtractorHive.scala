package bigframe.workflows.util

import org.apache.hadoop.hive.ql.exec.UDF

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

class SenExtractorHive extends UDF {
	
	private val senExtractor: SentimentExtractor = new SenExtractorSimple()
	
	def evaluate(s: String): Float = senExtractor.getSentiment(s)
	
}