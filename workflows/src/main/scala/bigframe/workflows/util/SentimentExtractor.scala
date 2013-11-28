package bigframe.workflows.util

abstract class SentimentExtractor {
	
	def getSentiment(text: String): Float
}