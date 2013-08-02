package bigframe.generator.datagen.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * To store the input information for each sub-knonecker graph to be generated.
 * 
 * @author andy
 * 
 */
public class KronGraphInfoWritable implements Writable {
	public int k;
	public int length;
	public int[][] path;

	public KronGraphInfoWritable() {
	}

	public KronGraphInfoWritable(int k, int[][] path) {
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

		for (int i = 0; i < length; i++) {
			for (int j = 0; j < 2; j++) {
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

		for (int i = 0; i < length; i++) {
			for (int j = 0; j < 2; j++) {
				out.writeInt(path[i][j]);
			}
		}
	}

	@Override
	public String toString() {
		return null;
	}
}

