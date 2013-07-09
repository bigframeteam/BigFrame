package edu.bigframe.datagen.nested;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PromotedProduct {
	private Date dateBegin;
	private Date dateEnd;
	private List<Integer> products;
	
	public PromotedProduct() {
		products = new ArrayList<Integer>();
	}
	
	public Date getDateBengin() {
		return dateBegin;
	}
	
	public Date getDateEnd() {
		return dateEnd;
	}
	
	public void addProduct(int prod) {
		products.add(prod);
	}
	
	public List<Integer> getProducts() {
		return products;
	}
	
	public void setProducts(List<Integer> prods) {
		products = prods;
	}
}
