package bigframe.util;

public abstract class Sorter {

	protected String input = "";
	protected String output = "";
	
	public Sorter(String in, String out) {
		input = in;
		output = out;
	}
	
	public String getInput() {
		return input;
	}
	
	public String getOutput() {
		return output;
	}
	
	public abstract void sort();
	
}
