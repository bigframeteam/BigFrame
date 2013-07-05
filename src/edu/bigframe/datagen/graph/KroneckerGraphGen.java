package edu.bigframe.datagen.graph;

import edu.bigframe.datagen.DatagenConf;
import edu.bigframe.util.Constants;

public abstract class KroneckerGraphGen extends GraphDataGen {

	protected double initial_graph[][];
	protected int num_rows;
	protected int num_columns;
	protected double expected_edges;
	protected int steps;
	protected String hdfs_path;
	
	private static final float arScaleVolume [] = 
		{0.12f, 12, 1200, 120000};
	
	public KroneckerGraphGen(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		initial_graph = new double[][]{
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
		
		num_rows = initial_graph.length;
		num_columns = initial_graph[0].length;
		assert( num_rows == num_columns );
		
		expected_edges = 0;
		for(int i = 0; i < num_rows;i++) {
			for(int j = 0; j < num_columns;j++) {
				expected_edges += initial_graph[i][j];
			}
		}
		
		hdfs_path = conf.getDataStoredPath().get(Constants.BIGFRAME_DATA_HDFSPATH_GRAPH) ;
		
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
