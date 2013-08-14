package bigframe.sentiment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;

/*
 Uses a mapping of word to sentiment score provided in a text file.
 Sentiment of a tweet is simply the sum of sentiment scores of constituent words.
*/
public class NaiveSentimentExtractor extends SentimentExtractor implements Serializable {

	private static final String RESOURCE_FILE = "AFINN-111.txt";

	private Map<String, Integer> scoresMap;

	/*
	 Reads resource file, and creates a hashmap of sentiment scores.
	*/
	public NaiveSentimentExtractor() {
		InputStream iStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_FILE);
		scoresMap = new HashMap<String, Integer>();
		try {
			BufferedReader br;
			br = new BufferedReader(new BufferedReader(new InputStreamReader(iStream)));
			
			String line;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				if (tokens != null) {
					try {
						String word = tokens[0];
						Integer score = Integer.parseInt(tokens[1]);
						scoresMap.put(word, score);
					} catch(Exception e) {
					
					}
				}

			}	
			
			br.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public double extract(String text) {
		double score = 0.0;
		String[] tokens = text.split(" ");
		for (String token: tokens) {
			if (scoresMap.containsKey(token)) {
				score += scoresMap.get(token).doubleValue();
			}
		}
		return score;
	}

}
