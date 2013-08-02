package bigframe.generator.enums;

public enum QueryVariety {
	SIMPLE,
	COMPLEX;
	
	/**
	 * @return a description for the fields
	 */
	public String getDescription() {

		switch (this) {
			case SIMPLE:
				return "Generate simple query";
			case COMPLEX:
				return "Generate complex data";

			default:
				return toString();
		}
	}
}
