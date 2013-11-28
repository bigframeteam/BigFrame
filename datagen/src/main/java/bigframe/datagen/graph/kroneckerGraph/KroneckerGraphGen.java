package bigframe.datagen.graph.kroneckerGraph;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.GraphDataGen;

/**
 * Abstract class for all knonecker graph generator.
 * 
 * @author andy
 * 
 */
public abstract class KroneckerGraphGen extends GraphDataGen {

	protected double initial_graph[][];
	protected int num_rows;
	protected int num_columns;
	protected double expected_edges;
	protected int steps;
	protected String hdfs_path;

	private static final float arScaleVolume [] = KnonGraphConstants.arScaleVolume2;
	private static final int arScaleNodeCount2 [] = KnonGraphConstants.arScaleNodeCount2;

	public KroneckerGraphGen(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		initial_graph = KnonGraphConstants.INITIAL_GRAPH2;

		num_rows = initial_graph.length;
		num_columns = initial_graph[0].length;
		assert( num_rows == num_columns );

		expected_edges = 0;
		for(int i = 0; i < num_rows;i++) {
			for(int j = 0; j < num_columns;j++) {
				expected_edges += initial_graph[i][j];
			}
		}

		hdfs_path = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_GRAPH) ;


	}

	public void setHDFSPATH(String path) {
		hdfs_path = path;
	}
	
	/**
	 * This method should be consistent with getNodeCount.
	 * @param targetGB
	 * @return
	 */
	public static float getRealGraphGB(float targetGB) {
		int i = 0;
		for(;; i++) {
			if (targetGB < arScaleVolume[0] * Math.pow(4, i) ) {
				break;
			}
		}

		if (i == 0)
			return arScaleVolume[0];

		else
			return (float) (arScaleVolume[0] * Math.pow(4, i -1));
	}

	public static long getNodeCount(float targetGB) {
		int i = 0;
		for(;; i++) {
			if (targetGB < arScaleVolume[0] * Math.pow(4, i)) {
				break;
			}
		}
		if (i==0)
			return arScaleNodeCount2[0];
		else
			return arScaleNodeCount2[0] * (long)Math.pow(2, i -1);
	}

	public abstract void setInitialGraph(double graph[][]);
}
