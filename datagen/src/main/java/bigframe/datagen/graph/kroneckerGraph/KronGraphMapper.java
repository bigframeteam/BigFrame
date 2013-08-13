package bigframe.datagen.graph.kroneckerGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

	@Override
	protected void map(NullWritable ignored,
			KronGraphInfoWritable krongrap_gen_info, final Context context)
					throws IOException, InterruptedException {
		/*
		 * LOG.info("Begin time: "+tweet_gen_info.begin+";" +"End time: " +
		 * tweet_gen_info.end + ";" + "Tweets per day: " +
		 * tweet_gen_info.tweets_per_day);
		 */
		Configuration mapreduce_config = context.getConfiguration();

		int k = krongrap_gen_info.k;
		int[][] path = krongrap_gen_info.path;
		steps = mapreduce_config.getInt(KnonGraphConstants.NUM_STEPS, 1);

		List<int[]> current_path = new ArrayList<int[]>();
		int length = path.length;
		for (int i = 0; i < length; i++) {
			current_path.add(path[i]);
		}

		int seed_offset = 12345;
		for (int i = 0; i < length; i++) {
			seed_offset = seed_offset * (path[i][0] - path[i][1]);
		}

		randnum = new Random();
		randnum.setSeed(RandomSeeds.SEEDS_TABLE[0] + seed_offset
				% RandomSeeds.SEEDS_TABLE[1]);
		kronRecursiveGen(k, current_path, context);
	}

	/**
	 * Generate the Stochastic Kronecker Graph recursively.
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
					if (d <= KnonGraphConstants.INITIAL_GRAPH[i][j]) {
						int real_row = i + 1;
						int real_column = j + 1;

						// Get the real row and column number in the generated
						// graph
						// based on the path information
						int exponent = steps - 1;
						int base = KnonGraphConstants.NUM_ROWS;
						for (int[] cell : path) {

							real_row += cell[0]
									* (int) Math.pow(base, exponent);
							real_column += cell[1]
									* (int) Math.pow(base, exponent);

							exponent--;
						}
						if (real_row == real_column)
							continue;
						try {
							context.write(null,
									new Text(String.valueOf(real_row) + "|"
											+ String.valueOf(real_column)));
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
					if (d <= KnonGraphConstants.INITIAL_GRAPH[i][j]) {
						List<int[]> new_path = new ArrayList<int[]>(path);
						int[] cell = { i, j };
						new_path.add(cell);

						kronRecursiveGen(k - 1, new_path, context);
					}
				}
			}

		}

	}
}
