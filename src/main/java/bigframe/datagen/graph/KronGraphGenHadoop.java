package bigframe.datagen.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import bigframe.BigConfConstants;
import bigframe.datagen.DatagenConf;
import bigframe.util.RandomSeeds;


public class KronGraphGenHadoop extends KroneckerGraphGen {



	public KronGraphGenHadoop(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void generate() {
		// TODO Auto-generated method stub

		System.out.println("Generating graph data");


		long nodeCount = KroneckerGraphGen.getNodeCount(targetGB);
		steps = (int) Math.log10(nodeCount);
		//float realgraphGB = KroneckerGraphGen.getRealGraphGB(targetGB);
		int nodePerMapper = KnonGraphConstants.NODEGEN_PER_MAPPER;
		int num_Mappers = (int) (nodeCount / nodePerMapper) * (int) (nodeCount / nodePerMapper);



		Configuration mapreduce_config = new Configuration();
		mapreduce_config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
		mapreduce_config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/mapred-site.xml"));
		mapreduce_config.setInt(KnonGraphConstants.NUM_STEPS, steps);
		mapreduce_config.setInt(KnonGraphConstants.NUM_MAPPERS, num_Mappers);


		try {
			Job job = new Job(mapreduce_config);

			Path outputDir = new Path(hdfs_dir);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setJarByClass(KronGraphGenHadoop.class);
			job.setMapperClass(KronGraphGenMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(GraphCellInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);


			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static class KronGraphGenMapper extends Mapper<NullWritable, KronGraphGenInfoWritable, NullWritable, Text> {
		//private static final Logger LOG = Logger.getLogger(KronGraphGenMapper.class);
		private Random randnum;
		private int steps;

		@Override
		protected void map(NullWritable ignored, KronGraphGenInfoWritable krongrap_gen_info, final Context context)
				throws IOException, InterruptedException {
			/*			LOG.info("Begin time: "+tweet_gen_info.begin+";"
				+"End time: " + tweet_gen_info.end + ";"
				+ "Tweets per day: " + tweet_gen_info.tweets_per_day);*/
			Configuration mapreduce_config = context.getConfiguration();

			int k = krongrap_gen_info.k;
			int [][] path = krongrap_gen_info.path;
			steps = mapreduce_config.getInt(KnonGraphConstants.NUM_STEPS, 1);

			List<int []> current_path = new ArrayList<int []>();
			int length = path.length;
			for(int i = 0; i < length; i++) {
				current_path.add(path[i]);
			}

			int seed_offset = 12345;
			for(int i = 0; i < length; i++) {
				seed_offset = seed_offset * (path[i][0] - path[i][1]) ;
			}

			randnum = new Random();
			randnum.setSeed(RandomSeeds.SEEDS_TABLE[0] + seed_offset % RandomSeeds.SEEDS_TABLE[1]);
			kronRecursiveGen(k, current_path, context );
		}
		/**
		 * Generate the Stochastic Kronecker Graph recursively.
		 * @param k
		 * @param path
		 * @return
		 */
		private void kronRecursiveGen(int k, List<int[]> path, final Context context ) {


			// Base case: Now, we finally reach a edge.
			if (k == 1) {
				for (int i = 0; i < KnonGraphConstants.NUM_ROWS; i++) {
					for (int j = 0; j < KnonGraphConstants.NUM_COLUMNS; j++) {
						double d = randnum.nextFloat();
						if(d <= KnonGraphConstants.INITIAL_GRAPH[i][j]) {
							int real_row = i+1;
							int real_column = j+1;

							//Get the real row and column number in the generated graph
							//based on the path information
							int exponent = steps - 1;
							int base = KnonGraphConstants.NUM_ROWS;
							for(int [] cell : path) {

								real_row += cell[0] * (int) Math.pow(base, exponent);
								real_column += cell[1] * (int) Math.pow(base, exponent);

								exponent--;
							}
							if(real_row == real_column)
								continue;
							try {
								context.write(null, new Text(String.valueOf(real_row)+"|"+String.valueOf(real_column)));
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

						//Recursively select a sub-region of the graph matrix
						if(d <= KnonGraphConstants.INITIAL_GRAPH[i][j]) {
							List<int []> new_path = new ArrayList<int []>(path);
							int [] cell = {i, j};
							new_path.add(cell);

							kronRecursiveGen(k-1, new_path, context);
						}
					}
				}

			}

		}

	}


	static class KronGraphGenInfoWritable implements Writable {
		public int k;
		public int length;
		public int [][] path;

		public KronGraphGenInfoWritable() {
		}

		public KronGraphGenInfoWritable(int k, int [][] path) {
			this.k = k;

			this.length = path.length;
			this.path = path;

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (null == in) {
				throw new IllegalArgumentException("in cannot be null");
			}
			k = in.readInt();
			length = in.readInt();

			for(int i = 0 ;i < length; i++) {
				for(int j = 0; j < 2; j++){
					path[i][j] = in.readInt();
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (null == out) {
				throw new IllegalArgumentException("out cannot be null");
			}
			out.writeInt(k);
			out.writeInt(length);

			for(int i = 0 ;i < length; i++) {
				for(int j = 0; j < 2; j++) {
					out.writeInt(path[i][j]);
				}
			}
		}

		@Override
		public String toString() {
			return null;
		}
	}

	static class GraphCellInputFormat  extends InputFormat<NullWritable, KronGraphGenInfoWritable> {
		/**
		 * An input split consisting of a range of time.
		 */
		private static final Logger LOG = Logger.getLogger(GraphCellInputFormat.class);
		static class GraphCellInputSplit extends InputSplit implements Writable {
			public int k;
			public int length;
			public int [][] path;

			public GraphCellInputSplit() { }

			public GraphCellInputSplit(int k, List<int []> path) {
				this.k = k;
				this.length = path.size();

				this.path = new int[length][2];



				for(int i = 0; i < this.length; i++ ) {
					this.path[i] = path.get(i);
					LOG.info("this path:" + this.path[i][0]+"," + this.path[i][1]);
				}
			}

			public long getLength() throws IOException {
				return 0;
			}

			public String[] getLocations() throws IOException {
				return new String[]{};
			}

			@Override
			public void readFields(DataInput in) throws IOException {
				// TODO Auto-generated method stub
				k = WritableUtils.readVInt(in);
				length = WritableUtils.readVInt(in);
				path = new int[length][2];

				if(this.path == null) {
					LOG.info("++++++++++++++++++++++++++++++++");
					LOG.error("path object is NULL");
					LOG.info("++++++++++++++++++++++++++++++++");
				}

				for(int i = 0 ;i < length; i++) {
					for(int j = 0; j < 2; j++){
						path[i][j] = WritableUtils.readVInt(in);
					}
				}

			}

			@Override
			public void write(DataOutput out) throws IOException {
				// TODO Auto-generated method stub
				WritableUtils.writeVInt(out, k);
				WritableUtils.writeVInt(out, length);
				for(int i = 0 ;i < length; i++) {
					for(int j = 0; j < 2; j++) {
						WritableUtils.writeVInt(out, path[i][j]);
					}
				}

			}
		}

		static class GraphCellRecordReader extends RecordReader<NullWritable, KronGraphGenInfoWritable> {

			private static final Logger LOG = Logger.getLogger(GraphCellInputFormat.class);

			int k;
			int [][] path;
			NullWritable key = null;
			KronGraphGenInfoWritable value = null;

			public GraphCellRecordReader() {
			}

			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {


				this.k = ((GraphCellInputSplit)split).k;
				int length = ((GraphCellInputSplit)split).length;

				if (length == 0)
					this.path = new int[0][2];
				else
					this.path = ((GraphCellInputSplit)split).path;

				if(this.path == null) {
					LOG.info("++++++++++++++++++++++++++++++++");
					LOG.error("path object is NULL");
					LOG.info("++++++++++++++++++++++++++++++++");
				}
			}

			public void close() throws IOException {
				// NOTHING
			}

			public NullWritable getCurrentKey() {
				return key;
			}

			public KronGraphGenInfoWritable getCurrentValue() {
				return value;
			}

			public float getProgress() throws IOException {
				return 0;
			}

			public boolean nextKeyValue() {
				if (key == null && value == null) {
					value = new KronGraphGenInfoWritable(k , path);
					return true;
				}
				else
					return false;

			}

		}

		@Override
		public RecordReader<NullWritable, KronGraphGenInfoWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new GraphCellRecordReader();
		}

		private  double logb( double a, double b )
		{
			return Math.log(a) / Math.log(b);
		}

		private List<InputSplit> getSplitRecursive(int k, List<int[]> path, int steps_left) {

			if(k == 1){
				List<InputSplit> splits = new ArrayList<InputSplit>();
				InputSplit temp_split = new GraphCellInputSplit(steps_left, path);
				splits.add(temp_split);

				return splits;
			}

			else {
				List<InputSplit> splits = new ArrayList<InputSplit>();
				k--;
				for(int i = 0; i < KnonGraphConstants.NUM_ROWS; i++){
					for(int j = 0; j < KnonGraphConstants.NUM_COLUMNS; j++){
						List<int []> new_path = new ArrayList<int []>(path);
						int [] cell = {i, j};
						new_path.add(cell);

						splits.addAll(getSplitRecursive(k, new_path, steps_left - 1 ));
					}
				}
				return splits;
			}
		}

		/**
		 * Create the desired number of splits, dividing the number of seconds
		 * between the mappers.
		 */
		public List<InputSplit> getSplits(JobContext job) {
			int num_Mappers = job.getConfiguration().getInt(KnonGraphConstants.NUM_MAPPERS, 1);
			int steps = job.getConfiguration().getInt(KnonGraphConstants.NUM_STEPS, 1);
			int k = (int) logb(num_Mappers, 100) + 1;

			List<int[]> path = new ArrayList<int []>();

			List<InputSplit> splits = getSplitRecursive(k, path, steps);

			return splits;
		}


	}
	@Override
	public void setInitialGraph(double[][] graph) {
		// TODO Auto-generated method stub

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
