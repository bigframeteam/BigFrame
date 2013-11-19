package bigframe.workflows.util;

import org.apache.hadoop.io.Text;

public class TextTextPair {

	private Text first;
	private Text second;
	
	public TextTextPair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	public void setFirst(Text value) {
		first = value;
	}
	
	public void setSecond(Text value) {
		second = value;
	}
}
