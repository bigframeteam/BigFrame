package edu.bigframe.datagen.text;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.bigframe.BigFrameDriver;
import edu.bigframe.datagen.DatagenConf;
import edu.bigframe.util.RandomSeeds;


/**
 * Generate Tweets with only sentiment words and product id.
 * @author andy
 *
 */
public class TweetTextGenSimple extends TweetTextGen {
	private List<String> sentiment_words;
	private int num_words;
	
	private Random randnum;
	
	public TweetTextGenSimple(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		
		//System.out.println(new File(".").getAbsolutePath());
		InputStream dictionary_file = BigFrameDriver.class.getClassLoader().getResourceAsStream("AFINN-111.txt");
		sentiment_words = new ArrayList<String>();
		randnum = new Random();
		randnum.setSeed(RandomSeeds.SEEDS_TABLE[0]);
		
		try {
			BufferedReader br;
			br = new BufferedReader(new BufferedReader(new InputStreamReader(dictionary_file)));
			
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
	public void generate() {
		// TODO Auto-generated method stub

	}


	@Override
	public String getNextTweet(int product_id) {
		// TODO Auto-generated method stub
		String tweet = String.valueOf(product_id);
		try {
			for (int i = 0; i < 10; i++) {
				int index = randnum.nextInt(num_words);
				tweet += " " + sentiment_words.get(index);
			}
		} catch( ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		
		return tweet;
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
