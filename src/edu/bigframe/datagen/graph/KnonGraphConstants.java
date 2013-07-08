package edu.bigframe.datagen.graph;

public class KnonGraphConstants {

	public static final String NUM_MAPPERS = "mapreduce.graph.num-mappers";
	public static final String NUM_STEPS = "mapreduce.graph.num-steps";
	
	public static final String GRAPH_TARGETGB =  "mapreduce.graph.targetGB";

	/**
	 * This two numbers must be consistent with the initial_graph. 
	 */
	public static final int NUM_ROWS = 10;
	public static final int NUM_COLUMNS = 10;
	
	
	public static final double[][] INITIAL_GRAPH = new double[][]{
			  { 1, 0.3, 0.2, 0, 0, 0, 0, 0.9, 0, 0 },
			  { 0.4, 1, 0, 0, 0, 0, 0, 0.4, 0, 0 },
			  { 0, 0, 1, 0, 0, 0, 0.2, 0, 0, 0 },
			  { 0.5, 0, 0, 1, 0, 0.1, 0, 0, 0, 0 },
			  { 0, 0, 0.2, 0, 1, 0, 0, 0.5, 0, 0 },
			  { 0, 0, 0, 0, 0, 1, 0, 0.1, 0, 0 },
			  { 0, 0, 0, 0, 0, 0, 1, 0, 0, 0 },
			  { 0, 0.1, 0.2, 0, 0, 0, 0, 1, 0, 0 },
			  { 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 },
			  { 0, 0, 0.2, 0, 0.1, 0, 0, 0, 0, 1 }
			};
	
	public static final float arScaleVolume [] = 
		{0.14f, 14, 1400, 140000};
	
	public static final int arScaleNodeCount [] = 
		{1000000, 10000000, 100000000, 1000000000};
	
	public static final int NODEGEN_PER_MAPPER = arScaleNodeCount[0];
}
