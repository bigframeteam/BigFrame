package bigframe.datagen.graph.kroneckerGraph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.util.RandomSeeds;


/**
 * Single machine implementation for the Stochastic Kronecker Graph Generator.
 * @author andy
 *
 */
public class KronGraphGenNaive extends KroneckerGraphGen {
	private int chunk_size;
	private int chunk_count;
	private Random randnum;
	
	public KronGraphGenNaive(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		
		steps = (int) Math.log10(KroneckerGraphGen.getNodeCount(targetGB));
		
		//Limit the number of edges in each file, to avoid out of memory issue
		chunk_size = 1000000;
		chunk_count = 0;
		
		// Initialize the random seed
		randnum = new Random();
		randnum.setSeed(RandomSeeds.SEEDS_TABLE[0]);
	}

	/**
	 * Generate the Stochastic Kronecker Graph recursively.
	 * @param k
	 * @param path
	 * @return
	 */
	private List<int []> kronRecursiveGen(int k, ArrayList<int []> path ) {
		
		
		// Base case: Now, we finally reach a edge.
		if (k == 1) {
			List<int []> edges = new LinkedList<int []>();
			for (int i = 0; i < num_rows; i++) {
				for (int j = 0; j < num_columns; j++) {
					double d = randnum.nextFloat();
					if(d <= initial_graph[i][j]) {
						int real_row = i+1;
						int real_column = j+1;
						
						//Get the real row and column number in the generated graph
						//based on the path information
						int exponent = steps - 1;
						int base = num_rows;
						for(int [] cell : path) {

							real_row += cell[0] * (int) Math.pow(base, exponent);
							real_column += cell[1] * (int) Math.pow(base, exponent);
							
							exponent--;
						}
						if(real_row == real_column)
							continue;
						//System.out.println(String.valueOf(real_row)+"|"+String.valueOf(real_column));
						
						int [] edge = {real_row, real_column};
						edges.add(edge);
					}
				}
			}
			return edges;
		}
		
		else {
			List<int []> edges = new LinkedList<int []>();
			for (int i = 0; i < num_rows; i++) {
				for (int j = 0; j < num_columns; j++) {
					double d = randnum.nextFloat();

					//Recursively select a sub-region of the graph matrix
					if(d <= initial_graph[i][j]) {
						ArrayList<int []> new_path = new ArrayList<int []>(path);
						int [] cell = {i, j};
						new_path.add(cell);
						
						
						edges.addAll(kronRecursiveGen(k-1, new_path));
					}
				}
			}
			if (edges.size() >= chunk_size) {
				System.out.println("Edges number:"+edges.size());
				Path stored_path = new Path(hdfs_path);
				Configuration config = new Configuration();
				config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
				FileSystem fileSystem;
				try {
					fileSystem = FileSystem.get(config);
					if (!fileSystem.exists(stored_path))
						fileSystem.mkdirs(stored_path);
					BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter
								(fileSystem.create(new Path(stored_path+"/twitter_graph.dat"+"."+chunk_count), true)));
					chunk_count++;
					
					for (int [] edge : edges) {
						bufferedWriter.write(edge[0]+"|"+edge[1]);
						bufferedWriter.newLine();
					}
					
					bufferedWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return new LinkedList<int []>();
			}
			else
				return edges;
		}
		
	}
	
	@Override
	public void generate() {
		// TODO Auto-generated method stub
		
		System.out.println("Generating Twitter Graph data");
		
		List<int []> graph = kronRecursiveGen(steps, new ArrayList<int []>() );
		//System.out.println(graph.size());
		
		if (graph.size() > 0) {
			Path path = new Path(hdfs_path);
			Configuration config = new Configuration();
			config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
			FileSystem fileSystem;
			try {
				fileSystem = FileSystem.get(config);
				fileSystem.mkdirs(path);
				BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter
							(fileSystem.create(new Path(path+"/twitter_graph.dat"+"."+chunk_count), true)));
				
				for (int [] edge : graph) {
					bufferedWriter.write(edge[0]+"|"+edge[1]);
					bufferedWriter.newLine();
				}
				bufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public void setInitialGraph(double[][] graph) {
		// TODO Auto-generated method stub
		initial_graph = graph;
	}

	@Override
	public int getAbsSizeBySF(int sf) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getSFbyAbsSize(int absSize) {
		// TODO Auto-generated method stub
		return 0;
	}


}
