package bigframe.datagen.text;

import java.util.HashMap;
import java.util.Map;

import bigframe.datagen.text.tweet.TweetTextGen;
import bigframe.datagen.text.tweet.TweetTextGenSimple;

/**
 * A factory for generate tweet text generator.
 * 
 * @author andy
 * 
 */
public class TextGenFactory {

	public static final Map<String, TweetTextGen> TWEETTEXTGEN_MAP;
	static {
		TWEETTEXTGEN_MAP = new HashMap<String, TweetTextGen>();
		TWEETTEXTGEN_MAP.put("simple", new TweetTextGenSimple(null, 0));

	}

	/**
	 * Return the tweet text generator by a given name.
	 * 
	 * @param textgen_name
	 * @return A instance of a tweet text generator.
	 */
	public static TweetTextGen getTextGenByName(String textgen_name) {
		return TWEETTEXTGEN_MAP.get(textgen_name);
	}
}
