package edu.bigframe.enums;

public enum DataVariety {
	RELATIONAL,
	GRAPH,
	NESTED,
	TEXT;
	
	/**
	 * @return a description for the fields
	 */
	public String getDescription() {

		switch (this) {
			case RELATIONAL:
				return "Generate relational data";
			case GRAPH:
				return "Generate graph data";
			case NESTED:
				return "Generate nested structure data";
			case TEXT:
				return "Generate text data";
			default:
				return toString();
		}
	}
}
