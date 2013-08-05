package bigframe.datagen.graph;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.DatagenConf;

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
	protected String hdfs_dir;

	private static final float arScaleVolume [] = KnonGraphConstants.arScaleVolume;

	public KroneckerGraphGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		initial_graph = KnonGraphConstants.INITIAL_GRAPH;

		num_rows = initial_graph.length;
		num_columns = initial_graph[0].length;
		assert( num_rows == num_columns );

		expected_edges = 0;
		for(int i = 0; i < num_rows;i++) {
			for(int j = 0; j < num_columns;j++) {
				expected_edges += initial_graph[i][j];
			}
		}

		hdfs_dir = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_GRAPH) ;


	}

	/**
	 * This method should be consistent with getNodeCount.
	 * @param targetGB
	 * @return
	 */
	public static float getRealGraphGB(float targetGB) {
		int i = 0;
		for(;i<arScaleVolume.length; i++) {
			if (targetGB < arScaleVolume[i]) {
				break;
			}
		}

		if (i == 0)
			return arScaleVolume[0];

		else
			return arScaleVolume[i];
	}

	public static long getNodeCount(float targetGB) {
		int i = 0;
		for(;i<arScaleVolume.length; i++) {
			if (targetGB < arScaleVolume[i]) {
				break;
			}
		}
		if (i==0)
			return (long) Math.pow(10, 6);
		else
			return (long) Math.pow(10, 6 + i -1);
	}

	public abstract void setInitialGraph(double graph[][]);
}
