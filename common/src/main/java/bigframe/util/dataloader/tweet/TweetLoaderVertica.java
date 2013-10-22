package bigframe.util.dataloader.tweet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.WorkflowInputFormat;

public class TweetLoaderVertica extends TweetDataLoader {

	public TweetLoaderVertica(WorkflowInputFormat workflowIF) {
		super(workflowIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean load(Path srcHdfsPath, String table, Configuration mapred_config) {
		
		return true;
	}

}
