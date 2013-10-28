package bigframe.datagen.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import bigframe.datagen.graph.kroneckerGraph.KnonGraphConstants;
import bigframe.datagen.graph.kroneckerGraph.KronGraphInfoWritable;

/**
 * Class for recording which part of the Knonecker Graph the Mapper is
 * generating.
 * 
 * @author andy
 * 
 */
public class GraphCellInputFormat extends
InputFormat<NullWritable, KronGraphInfoWritable> {
	/**
	 * An input split consisting of a range of time.
	 */
	private static final Logger LOG = Logger
			.getLogger(GraphCellInputFormat.class);

	static class GraphCellInputSplit extends InputSplit implements Writable {
		public int k;
		public int length;
		public int[][] path;

		public GraphCellInputSplit() {
		}

		public GraphCellInputSplit(int k, List<int[]> path) {
			this.k = k;
			this.length = path.size();

			this.path = new int[length][2];

			for (int i = 0; i < this.length; i++) {
				this.path[i] = path.get(i);
				LOG.info("this path:" + this.path[i][0] + "," + this.path[i][1]);
			}
		}

		public long getLength() throws IOException {
			return 0;
		}

		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			k = WritableUtils.readVInt(in);
			length = WritableUtils.readVInt(in);
			path = new int[length][2];

			if (this.path == null) {
				LOG.info("++++++++++++++++++++++++++++++++");
				LOG.error("path object is NULL");
				LOG.info("++++++++++++++++++++++++++++++++");
			}

			for (int i = 0; i < length; i++) {
				for (int j = 0; j < 2; j++) {
					path[i][j] = WritableUtils.readVInt(in);
				}
			}

		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			WritableUtils.writeVInt(out, k);
			WritableUtils.writeVInt(out, length);
			for (int i = 0; i < length; i++) {
				for (int j = 0; j < 2; j++) {
					WritableUtils.writeVInt(out, path[i][j]);
				}
			}

		}
	}

	static class GraphCellRecordReader extends
	RecordReader<NullWritable, KronGraphInfoWritable> {

		private static final Logger LOG = Logger
				.getLogger(GraphCellInputFormat.class);

		int k;
		int[][] path;
		NullWritable key = null;
		KronGraphInfoWritable value = null;

		public GraphCellRecordReader() {
		}

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			this.k = ((GraphCellInputSplit) split).k;
			int length = ((GraphCellInputSplit) split).length;

			if (length == 0)
				this.path = new int[0][2];
			else
				this.path = ((GraphCellInputSplit) split).path;

			if (this.path == null) {
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

		public KronGraphInfoWritable getCurrentValue() {
			return value;
		}

		public float getProgress() throws IOException {
			return 0;
		}

		public boolean nextKeyValue() {
			if (key == null && value == null) {
				value = new KronGraphInfoWritable(k, path);
				return true;
			} else
				return false;

		}

	}

	@Override
	public RecordReader<NullWritable, KronGraphInfoWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new GraphCellRecordReader();
	}

	private double logb(double a, double b) {
		return Math.log(a) / Math.log(b);
	}

	private List<InputSplit> getSplitRecursive(int k, List<int[]> path,
			int steps_left) {

		if (k == 1) {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			InputSplit temp_split = new GraphCellInputSplit(steps_left, path);
			splits.add(temp_split);

			return splits;
		}

		else {
			List<InputSplit> splits = new ArrayList<InputSplit>();
			k--;
			for (int i = 0; i < KnonGraphConstants.NUM_ROWS; i++) {
				for (int j = 0; j < KnonGraphConstants.NUM_COLUMNS; j++) {
					List<int[]> new_path = new ArrayList<int[]>(path);
					int[] cell = { i, j };
					new_path.add(cell);

					splits.addAll(getSplitRecursive(k, new_path, steps_left - 1));
				}
			}
			return splits;
		}
	}

	/**
	 * Create the desired number of splits, each mapper generate
	 * the minimum graph size.
	 */
	public List<InputSplit> getSplits(JobContext job) {
		int num_Mappers = job.getConfiguration().getInt(
				KnonGraphConstants.NUM_MAPPERS, 1);
		int steps = job.getConfiguration().getInt(KnonGraphConstants.NUM_STEPS,
				1);
		int k = (int) logb(num_Mappers, 4) + 1;

		List<int[]> path = new ArrayList<int[]>();

		List<InputSplit> splits = getSplitRecursive(k, path, steps);

		return splits;
	}

}
