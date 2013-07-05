package edu.bigframe.enums;

public enum QueryVelocity {
	HISTORICAL,
	REPEATED_BATCH,
	CONTINUOUS;
	
	/**
	 * @return a description for the fields
	 */
	public String getDescription() {

		switch (this) {
			case HISTORICAL:
				return "Generate historical queries";
			case REPEATED_BATCH:
				return "Generate repeated batch queries";
			case CONTINUOUS:
				return "Generate continuous queries";
			default:
				return toString();
		}
	}
}
