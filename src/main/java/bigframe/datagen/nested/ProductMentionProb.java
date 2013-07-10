package bigframe.datagen.nested;

import java.util.ArrayList;
import java.util.List;


public class ProductMentionProb{
	protected List<Integer> customers;
	protected List<Integer> non_customers;
	
	protected double cust_mention_prob;
	protected double non_cust_mention_prob;
	
	protected double promotion_cust_mention_prob;
	protected double promotion_non_cust_mention_prob;
	
	protected double promoted_prod_men_prob_cust;
	protected double promoted_prod_men_prob_noncust;
	
	protected PromotedProduct promoted_products;
	
	public ProductMentionProb() {
		customers = new ArrayList<Integer>();
		non_customers = new ArrayList<Integer>();
		
		cust_mention_prob = 0.2;
		non_cust_mention_prob = 0.02;
		
		promotion_cust_mention_prob  = 0.8;
		promotion_non_cust_mention_prob = 0.08;
		promoted_prod_men_prob_cust = 0.8;
		promoted_prod_men_prob_noncust = 0.2;
	}
	
	public double getCustMenProb() {
		return cust_mention_prob;
	}
	
	public double getNonCustMenProb() {
		return non_cust_mention_prob;
	}
	
	public double getPromotedProdMenProbCust() {
		return promoted_prod_men_prob_cust;
	}
	
	public double getPromotedProdMenProbNonCust() {
		return promoted_prod_men_prob_noncust;
	}
}
