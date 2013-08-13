package bigframe.queries;

import org.apache.hadoop.conf.Configuration;

public interface HadoopRunnable {

	public void run(Configuration mapred_config);
}
