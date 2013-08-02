package bigframe.generator.datagen.nested;

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
	public Long tweets_per_mapper;
	public Long tweet_start_ID;

	public RawTweetInfoWritable() {
	}

	public RawTweetInfoWritable(Long begin, Long end, Long tweets_per_mapper, Long tweet_start_ID) {
		this.begin = begin;
		this.end = end;
		this.tweets_per_mapper = tweets_per_mapper;
		this.tweet_start_ID = tweet_start_ID;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (null == in)
			throw new IllegalArgumentException("in cannot be null");

		begin = in.readLong();
		end = in.readLong();
		tweets_per_mapper = in.readLong();
		tweet_start_ID = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (null == out)
			throw new IllegalArgumentException("out cannot be null");
		out.writeLong(begin);
		out.writeLong(end);
		out.writeLong(tweets_per_mapper);
		out.writeLong(tweet_start_ID);
	}

	@Override
	public String toString() {
		return "Time begin: " + begin + "\n" + "Time end: " + end + "\n"
				+ "Tweets per mapper: " + tweets_per_mapper + "\n"
				+ "Tweet start ID" + tweet_start_ID + "\n";
	
	}
}
