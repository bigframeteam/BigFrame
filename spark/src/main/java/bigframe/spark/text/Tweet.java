/**
 * 
 */
package bigframe.spark.text;

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.io.Serializable;

/**
 * @author kunjirm
 *
 */

class User implements Serializable{

	@SerializedName("id")
	double id;

	@SerializedName("name")
	String name;

	public User(User user) {
		this.id = user.getId();
		this.name = user.getName();
	}

	public double getId() {
		return id;
	}

	public void setId(double id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}

class Entity implements Serializable{
	
	@SerializedName("hashtags")
	List<String> products;
	
	public Entity(Entity entity)
	{
		this.products = entity.getProducts();
	}

	public List<String> getProducts() {
		return products;
	}

	public void setProducts(List<String> products) {
		this.products = products;
	}

}


public class Tweet implements Serializable{

	@SerializedName("text")
	String text;

	@SerializedName("id")
	String id;

	@SerializedName("created_at")
	String creationTime;

	@SerializedName("user")
	User user;

	@SerializedName("entities")
	Entity entity;

	transient int productID;
	transient double score;

	public Tweet(Tweet tweet) {
		this.text = tweet.getText();
		this.id = tweet.getId();
		this.creationTime = tweet.getCreationTime();
		this.user = new User(tweet.getUser());
		this.entity = tweet.getEntity();
		this.productID = tweet.getProductID();
		this.score = tweet.getScore();
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		System.out.println("Setting text: " + text);
		this.text = text;
		if(text != null && text != "") {
			productID = Integer.parseInt(text.split(" ")[0]);
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(String creationTime) {
		this.creationTime = creationTime;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public String getUserName() {
		return user.getName();
	}

	public double getUserId() {
		return user.getId();
	}
	
	public Entity getEntity() {
		return entity;
	}
	
	public void setEntity(Entity entity) {
		this.entity = entity;
	}

	public List<String> getProductNames() {
		return entity.getProducts();
	}

	public int getProductID() {
		if(productID == 0 && text != null && text != "") {
			productID = Integer.parseInt(text.split(" ")[0]);
		}
		return productID;
	}

	public void setProductID(int productID) {
		this.productID = productID;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
}
