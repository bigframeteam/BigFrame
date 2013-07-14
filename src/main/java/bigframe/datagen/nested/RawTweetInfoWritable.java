package bigframe.datagen.nested;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Class for recording which time range of tweets a mapper shoud generate.
 * 
 * @author andy
 * 
 */
public class RawTweetInfoWritable implements Writable {
	public Long begin;
	public Long end;
	public Long tweets_per_day;

	public RawTweetInfoWritable() {
	}

	public RawTweetInfoWritable(Long begin, Long end, Long tweets_per_day) {
		this.begin = begin;
		this.end = end;
		this.tweets_per_day = tweets_per_day;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (null == in)
			throw new IllegalArgumentException("in cannot be null");

		begin = in.readLong();
		end = in.readLong();
		tweets_per_day = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (null == out)
			throw new IllegalArgumentException("out cannot be null");
		out.writeLong(begin);
		out.writeLong(end);
		out.writeLong(tweets_per_day);
	}

	@Override
	public String toString() {
		return "Time begin: " + begin + "\n" + "Time end: " + end + "\n"
				+ "Tweets per day: " + tweets_per_day;
	}
}
