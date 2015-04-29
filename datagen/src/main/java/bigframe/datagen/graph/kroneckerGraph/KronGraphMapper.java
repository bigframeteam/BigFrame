package bigframe.datagen.graph.kroneckerGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskID;

import bigframe.datagen.util.RandomSeeds;

/**
 * Mapper for generate each sub part of the kronecker graph.
 * 
 * @author andy
 * 
 */
public class KronGraphMapper extends
Mapper<NullWritable, KronGraphInfoWritable, NullWritable, Text> {
	// private static final Logger LOG =
	// Logger.getLogger(KronGraphGenMapper.class);
	private Random randnum;
	private int steps;

	//@Override
	protected void map(NullWritable ignored,
			KronGraphInfoWritable krongrap_gen_info, final Context context)
					throws IOException, InterruptedException {

		Configuration mapreduce_config = context.getConfiguration();

		int k = krongrap_gen_info.k;
		int[][] path = krongrap_gen_info.path;
		steps = mapreduce_config.getInt(KnonGraphConstants.NUM_STEPS, 1);

		List<int[]> current_path = new ArrayList<int[]>();
		int length = path.length;
		for (int i = 0; i < length; i++) {
			current_path.add(path[i]);
		}

		// Before each map split has a different random seed.
		// But it is not easy to control the graph size, so we just make it the same.
//		int init_offset = 234231781;
		int seed_offset = 0;
//		for (int i = 0; i < length; i++) {
//			seed_offset += init_offset * (path[i][0] * 3 + path[i][1]);
//			seed_offset = seed_offset % RandomSeeds.SEEDS_TABLE[1];
//		}

		
		randnum = new Random();
		randnum.setSeed(seed_offset);
		kronRecursiveGen(k, current_path, context);
	}

	/**
	 * Generate the Stochastic Kronecker Graph recursively.
	 * The implementation is following this paper:
	 *   "Kronecker Graphs: An Approach to Modeling Networks"
	 * 
	 * @param k
	 * @param path
	 * @return
	 */
	private void kronRecursiveGen(int k, List<int[]> path, final Context context) {

		// Base case: Now, we finally reach a edge.
		if (k == 1) {
			for (int i = 0; i < KnonGraphConstants.NUM_ROWS; i++) {
				for (int j = 0; j < KnonGraphConstants.NUM_COLUMNS; j++) {
					double d = randnum.nextFloat();
					if (d <= KnonGraphConstants.INITIAL_GRAPH2[i][j]) {
						long real_row =  (i + 1);
						long real_column = (j + 1);

						// Get the real row and column number in the generated
						// graph  based on the path information.
						// 
						int exponent = steps - 1;
						int base = KnonGraphConstants.NUM_ROWS;
						for (int[] cell : path) {

						  // TODO Overflow checking!!!
							real_row += (long) (cell[0] * Math.pow(base, exponent)) ;
							real_column += (long) (cell[1] * Math.pow(base, exponent));
							
							exponent--;
						}
						if (real_row == real_column)
							continue;
						
					  // TODO Overflow checking!!!
						// Offset the node ID such that the size of a node is consistent.
            real_row += KnonGraphConstants.OFFSET;
            real_column += KnonGraphConstants.OFFSET; 
						
						try {
							context.write(null,
									new Text(real_row + "|" + real_column));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
			}
		}

		else {
			for (int i = 0; i < KnonGraphConstants.NUM_ROWS; i++) {
				for (int j = 0; j < KnonGraphConstants.NUM_COLUMNS; j++) {
					double d = randnum.nextFloat();

					// Recursively select a sub-region of the graph matrix
					if (d <= KnonGraphConstants.INITIAL_GRAPH2[i][j]) {
						List<int[]> new_path = new ArrayList<int[]>(path);
						int[] cell = { i, j };
						// Keep track of which sub-region we have selected.
						new_path.add(cell);

						kronRecursiveGen(k - 1, new_path, context);
					}
				}
			}

		}

	}
}
