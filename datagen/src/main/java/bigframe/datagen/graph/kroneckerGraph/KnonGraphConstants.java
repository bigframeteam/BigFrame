package bigframe.datagen.graph.kroneckerGraph;

/**
 * Some constants used by knongraph generator.
 * 
 * @author andy
 *
 */
public class KnonGraphConstants {

	public static final String NUM_MAPPERS = "mapreduce.graph.num-mappers";
	public static final String NUM_STEPS = "mapreduce.graph.num-steps";
	
	public static final String GRAPH_TARGETGB =  "mapreduce.graph.targetGB";

	/**
	 * This two numbers must be consistent with the initial_graph. 
	 */
	public static final int NUM_ROWS = 2;
	public static final int NUM_COLUMNS = 2;
	
	/**
	 * The initial graph for knonecker graph generator with size 10
	 */
//	public static final double[][] INITIAL_GRAPH10 = new double[][]{
//			  { 1, 0.3, 0.2, 0, 0, 0, 0, 0.9, 0, 0 },
//			  { 0.4, 1, 0, 0, 0, 0, 0, 0.4, 0, 0 },
//			  { 0, 0, 1, 0, 0, 0, 0.2, 0, 0, 0 },
//			  { 0.5, 0, 0, 1, 0, 0.1, 0, 0, 0, 0 },
//			  { 0, 0, 0.2, 0, 1, 0, 0, 0.5, 0, 0 },
//			  { 0, 0, 0, 0, 0, 1, 0, 0.1, 0, 0 },
//			  { 0, 0, 0, 0, 0, 0, 1, 0, 0, 0 },
//			  { 0, 0.1, 0.2, 0, 0, 0, 0, 1, 0, 0 },
//			  { 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 },
//			  { 0, 0, 0.2, 0, 0.1, 0, 0, 0, 0, 1 }
//			};
	
	/**
	 * The initial graph for knonecker graph generator with size 2
	 */
	public static final double[][] INITIAL_GRAPH2 = new double[][]{
				{0.9999, 0.5413},
				{0.631, 0.1612}
			};
	
//	public static final float arScaleVolume10 [] = 
//		{0.14f, 14, 1400, 140000};
//	
//	public static final int arScaleNodeCount10 [] = 
//		{1000000, 10000000, 100000000, 1000000000};
	
	// size in GB for the minimum graph
	public static final float arScaleVolume2 [] = 
		{0.32f};
	
	// 2^18 nodes
	public static final int arScaleNodeCount2 [] = 
		{524288};
	
	public static final int NODEGEN_PER_MAPPER = arScaleNodeCount2[0];
}
