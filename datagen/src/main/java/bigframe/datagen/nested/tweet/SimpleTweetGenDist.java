package bigframe.datagen.nested.tweet;


import java.text.SimpleDateFormat;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import bigframe.datagen.text.tweet.TweetTextGen;

/**
 * A simple distribution to control tweet generation.
 * Including:
 * 1. Which twitter user mention this tweet.
 * 2. If a product is mentioned in the tweet
 * 3. If a promoted product is mentioned. 
 * 
 * @author andy
 *
 */
public class SimpleTweetGenDist extends TweetGenDist {

	private double cust_mention_prob;
	private double noncust_mention_prob;
	
//	private double promotion_cust_mention_prob;
//	private double promotion_non_cust_mention_prob;
	
	private double promoted_prod_men_prob_cust;
	private double promoted_prod_men_prob_noncust;
	
	public SimpleTweetGenDist(long random_seed, TweetTextGen text_gen, long ID) {
		super(random_seed, text_gen, ID);
		
		cust_mention_prob = 0.8;
		noncust_mention_prob = 0.08;
		
//		promotion_cust_mention_prob  = 0.8;
//		promotion_non_cust_mention_prob = 0.08;
		
		promoted_prod_men_prob_cust = 0.8;
		promoted_prod_men_prob_noncust = 0.2;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getNextTweet() {
		
		double ratio = cust_twitter_acc.length /(cust_twitter_acc.length + noncust_twitter_acc.length);
		
		SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
		//Random choose a time stamp
		long timestamp = time_begin + random.nextInt((int)(time_end - time_begin + 1));
		

		
		double flip;
		flip = random.nextDouble();
		
		long user_id;
		int prod_id;
		// if yes, choose a customer
		if (flip <= ratio) {
			int u_index = random.nextInt(cust_twitter_acc.length);
			user_id = cust_twitter_acc[u_index];
			
			flip = random.nextDouble();
			// If yes, the customer will mention our product in her tweet
			if(flip <= cust_mention_prob) {			
				flip = random.nextDouble();
				// If yes, she will mention the promoted product
				if(flip <= promoted_prod_men_prob_cust) {
					int p_index = random.nextInt(promt_info.getProductSK().size());
					prod_id = promt_info.getProductSK().get(p_index);
				}
				else {
					int p_index = random.nextInt(totalnum_prods);
					prod_id = p_index + 1;
				}
			}
			else {
				prod_id = -1;
			}
		}
		
		else {
			int index = random.nextInt(noncust_twitter_acc.length);
			user_id = noncust_twitter_acc[index];
			
			flip = random.nextDouble();
			if(flip <= noncust_mention_prob) {	
				flip = random.nextDouble();
				if(flip <= promoted_prod_men_prob_noncust) {
					int p_index = random.nextInt(promt_info.getProductSK().size());
					prod_id = promt_info.getProductSK().get(p_index);
				}
				else {
					int p_index = random.nextInt(totalnum_prods);
					prod_id = p_index + 1;
				}
			}
			else {
				prod_id = -1;
			}
		}
		
		// Begin to assign the chosen values

		
		String tweet = text_gen.getNextTweet(prod_id);
		String date = twitterDateFormat.format(timestamp*1000);

		
		tweet_json.put("created_at", date);
		tweet_json.put("text", tweet);
		tweet_json.put("id", String.valueOf(tweet_startID));

		tweet_startID++;
		// How to put nested attribute?
		JSONObject user_json = (JSONObject) tweet_json.get("user");
		user_json.put("id", user_id);
		tweet_json.put("user", user_json);
		
		JSONObject entities_json = (JSONObject) tweet_json.get("entities");

		
		if(prod_id != -1) {
			 assert prod_id > 0;
			 String prod_name = item_info.getProdName().get(prod_id-1);
			 JSONArray list = new JSONArray();
			 list.add(prod_name);
			 entities_json.put("hashtags", list);
		}
		else {
			 JSONArray list = new JSONArray();
			 list.add("No product mentioned");
			 entities_json.put("hashtags", list);
		}
		
		System.out.println("entities: "+entities_json);
		tweet_json.put("entities", entities_json);
		
		return tweet_json.toString();
	}

	public double getCustMenProb() {
		return cust_mention_prob;
	}
	
	public double getNonCustMenProb() {
		return noncust_mention_prob;
	}
	
//	public double getPromotionCustMenProb() {
//		return promotion_cust_mention_prob;
//	}
//	
//	public double getPromotionNonCustMenProb() {
//		return promotion_non_cust_mention_prob;
//	}
	
	public double getPromotedProdMenProbCust() {
		return promoted_prod_men_prob_cust;
	}
	
	public double getPromotedProdMenProbNonCust() {
		return promoted_prod_men_prob_noncust;
	}

	
}
