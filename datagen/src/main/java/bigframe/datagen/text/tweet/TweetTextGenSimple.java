package bigframe.datagen.text.tweet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.util.RandomUtil;



/**
 * Generate Tweets with only sentiment words.
 * @author andy
 *
 */
public class TweetTextGenSimple extends TweetTextGen {
	private List<String> sentiment_words;
	private int num_words;

	private Random randnum;

	public TweetTextGenSimple(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);

		InputStream dictionary_file = TweetTextGenSimple.class.getClassLoader().getResourceAsStream("AFINN-111.txt");
		sentiment_words = new ArrayList<String>();
		randnum = new Random();
		randnum.setSeed(RandomUtil.SEEDS_TABLE[0]);

		try {
			BufferedReader br;
			br = new BufferedReader(new InputStreamReader(dictionary_file));

			String line;
			while ((line = br.readLine()) != null) {
				String word = line.split("\t")[0];
				if (word != null) {
					sentiment_words.add(word);
				}

			}

			num_words = sentiment_words.size();
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
	public void setRandomSeed(long seed) {
		randnum.setSeed(seed);
	}

	@Override
	public void generate() {
		// TODO Auto-generated method stub

	}


	@Override
	public String getNextTweet(int product_id) {
		// Currently, the product id doesn't appear in the text.
	  // Instead, we mark the product name in the hashtags 
		StringBuilder tweet = new StringBuilder();
		try {
			for (int i = 0; i < 10; i++) {
				int index = randnum.nextInt(num_words);
				tweet.append(sentiment_words.get(index)).append(" ");
			}
		} catch( ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		return tweet.toString();
	}


	@Override
	public int getAbsSizeBySF(int sf) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int getSFbyAbsSize(int absSize) {
		// TODO Auto-generated method stub
		return 0;
	}


}
