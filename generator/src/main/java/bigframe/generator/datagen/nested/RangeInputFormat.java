package bigframe.generator.datagen.nested;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Store which time range of the tweets that a mapper should generate.
 * 
 * @author andy
 * 
 */
public class RangeInputFormat extends
InputFormat<NullWritable, RawTweetInfoWritable> {

	/**
	 * An input split consisting of a range of time.
	 */
	// private static final Logger LOG = Logger
	// .getLogger(RangeInputFormat.class);

	static class RangeInputSplit extends InputSplit implements Writable {
		long begin;
		long end;
		long tweets_per_mapper;
		long tweet_start_ID;

		public RangeInputSplit() {
		}

		public RangeInputSplit(long begin, long end, long tweets_per_mapper, long tweet_start_ID) {
			this.begin = begin;
			this.end = end;
			this.tweets_per_mapper = tweets_per_mapper;
			this.tweet_start_ID = tweet_start_ID;
		}

		@Override
		public long getLength() throws IOException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			begin = WritableUtils.readVLong(in);
			end = WritableUtils.readVLong(in);
			tweets_per_mapper = WritableUtils.readVLong(in);
			tweet_start_ID = WritableUtils.readVLong(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			WritableUtils.writeVLong(out, begin);
			WritableUtils.writeVLong(out, end);
			WritableUtils.writeVLong(out, tweets_per_mapper);
			WritableUtils.writeVLong(out, tweet_start_ID);
		}
	}

	static class RangeRecordReader extends
	RecordReader<NullWritable, RawTweetInfoWritable> {

		long begin;
		long end;
		long tweets_per_mapper;
		long tweet_start_ID;
		NullWritable key = null;
		RawTweetInfoWritable value = null;

		public RangeRecordReader() {
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			begin = ((RangeInputSplit) split).begin;
			end = ((RangeInputSplit) split).end;
			tweets_per_mapper = ((RangeInputSplit) split).tweets_per_mapper;
			tweet_start_ID = ((RangeInputSplit) split).tweet_start_ID;
		}

		@Override
		public void close() throws IOException {
			// NOTHING
		}

		@Override
		public NullWritable getCurrentKey() {
			return key;
		}

		@Override
		public RawTweetInfoWritable getCurrentValue() {
			return value;
		}

		@Override
		public float getProgress() throws IOException {
			return 0;
		}

		@Override
		public boolean nextKeyValue() {
			if (key == null && value == null) {
				value = new RawTweetInfoWritable(begin, end, tweets_per_mapper, tweet_start_ID);
				return true;
			} else
				return false;

		}

	}

	@Override
	public RecordReader<NullWritable, RawTweetInfoWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new RangeRecordReader();
	}

	/**
	 * Create the desired number of splits, dividing the number of seconds
	 * between the mappers.
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) {
		long time_begin = getTimeBegin(job);
		long time_end = getTimeEnd(job);
		long total_time = time_end - time_begin;
		int numSplits = job.getConfiguration().getInt(
				RawTweetGenConstants.NUM_MAPPERS, 1);
		int tweets_per_mapper = job.getConfiguration().getInt(
				RawTweetGenConstants.TWEETS_PER_MAPPER, 1);
		// LOG.info("Generating total seconds " + total_time + " using "
		// + numSplits);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long begin = time_begin;
		long tweet_start_ID = 1;
		long duration = (long) Math.ceil(total_time * 1.0 / numSplits);
		for (int split = 0; split < numSplits; ++split) {
			splits.add(new RangeInputSplit(begin, duration + begin,
					tweets_per_mapper, tweet_start_ID));
			begin += duration;
			tweet_start_ID += tweets_per_mapper;
		}
		return splits;
	}

	public long getTimeBegin(JobContext job) {
		return job.getConfiguration().getLong(RawTweetGenConstants.TIME_BEGIN,
				0);
	}

	public void setTimeBegin(Job job, long time_begin) {
		job.getConfiguration().setLong(RawTweetGenConstants.TIME_BEGIN,
				time_begin);
	}

	public long getTimeEnd(JobContext job) {
		return job.getConfiguration().getLong(RawTweetGenConstants.TIME_END, 0);
	}

	public void setTimeEnd(Job job, long time_end) {
		job.getConfiguration().setLong(RawTweetGenConstants.TIME_END, time_end);
	}
}
