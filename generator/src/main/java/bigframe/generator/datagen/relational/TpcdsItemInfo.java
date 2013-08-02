package bigframe.generator.datagen.relational;

import java.util.ArrayList;
import java.util.List;

public class TpcdsItemInfo {
	private List<String> item_ids;
	private List<String> prod_names;
	
	public TpcdsItemInfo() {
		item_ids = new ArrayList<String>();
		prod_names = new ArrayList<String>();
	}
	
	public void addItemId(String ID) {
		item_ids.add(ID);
	}
	
	public void addProdName(String name) {
		prod_names.add(name);
	}
	
	public List<String> getItemIDs() {
		return item_ids;
	}
	
	public List<String> getProdName() {
		return prod_names;
	}
	
	public void setItemIDs(List<String> item_ids) {
		this.item_ids = item_ids;
	}
	
	public void setProdNames(List<String> prod_names) {
		this.prod_names = prod_names;
	}
	
}
