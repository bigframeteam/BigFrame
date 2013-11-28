package bigframe.workflows.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TextDoublePair {
	
	private Text first;
	private DoubleWritable second;
	
	public TextDoublePair(Text first, DoubleWritable second) {
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public DoubleWritable getSecond() {
		return second;
	}
	
	public void setFirst(Text value) {
		first = value;
	}
	
	public void setSecond(DoubleWritable value) {
		second = value;
	}

}
