package bigframe.sentiment;

/*
 Abstract class for sentiment extraction
*/
public abstract class SentimentExtractor {

	/*
	 Given a sentence, returns its sentiment score.
	*/
	public abstract double extract(String text);

}
