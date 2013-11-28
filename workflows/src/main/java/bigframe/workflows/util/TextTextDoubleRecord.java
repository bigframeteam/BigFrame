package bigframe.workflows.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TextTextDoubleRecord {

	private Text first;
	private Text second;
	private DoubleWritable value;
	
	public TextTextDoubleRecord(Text first, Text second, DoubleWritable value) {
		this.first = first;
		this.second = second;
		this.value = value;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	public DoubleWritable getValue() {
		return value;
	}
	
	public void setFirst(Text value) {
		first = value;
	}
	
	public void setSecond(Text value) {
		second = value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}
}
