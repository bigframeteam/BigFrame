package bigframe.workflows.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

	private String HADOOP_HOME;
	private FileSystem fs;
	private Configuration mapred_config;
	
	public HDFSUtil(String hadoop_home) {
		HADOOP_HOME = hadoop_home;
		
		mapred_config = new Configuration();
		mapred_config.addResource(new Path(HADOOP_HOME
				+ "/conf/core-site.xml"));
		mapred_config.addResource(new Path(HADOOP_HOME
				+ "/conf/mapred-site.xml"));
		
		try {
			fs = FileSystem.get(mapred_config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteFile(String path) {
		Path delteDir = new Path(path);	

		try {
			if(fs.exists(delteDir))
				fs.delete(delteDir, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
