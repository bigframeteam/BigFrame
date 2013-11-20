package bigframe.datagen.relational.tpcds;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenerator;
import bigframe.datagen.relational.RelationalDataGen;
import bigframe.datagen.relational.twittermapping.TwitterMappingGenNaive;

/**
 * A naive parallel implementation of tpcds data generator. 
 * The main idea is to call a external script, which will finally, delegate
 * the real generation procedure to the tpcds generation program.
 *  
 * @author andy
 *
 */
public class TpcdsDataGenNaive extends TpcdsDataGen {
	private static final Logger LOG = Logger.getLogger(TpcdsDataGenNaive.class);
	
	private int num_files;
	private String hdfs_path;
	private String local_dir;
	private String datagen_script;
	private String hadoop_slaves;
	private String dagegen_script_path;
	
	TwitterMappingGenNaive twitter_mapping;
	
	public TpcdsDataGenNaive(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		
		if(targetGB < 1) {
			LOG.error("The size of relational data cannot be less than 1GB. \n" +
					"please modified the big data volume!");
			System.exit(1);
			
		}
		
		twitter_mapping = new TwitterMappingGenNaive(conf,targetGB);
		
		num_files = (int) (targetGB/2) + 1;
		if (num_files < 2) {
			num_files = 2;
		}

		hdfs_path = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL);
		local_dir = conf.getProp().get(BigConfConstants.BIGFRAME_TPCDS_LOCAL);
		datagen_script = conf.getProp().get(BigConfConstants.BIGFRAME_TPCDS_SCRIPT);
		hadoop_slaves = conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_SLAVE);
		
		dagegen_script_path = (new File(datagen_script)).getParentFile().getAbsolutePath();
	}

	public void setHDFSPATH(String path) {
		hdfs_path = path;
		twitter_mapping.setHDFS_PATH(path + "/" + "twitter_mapping");
	}
	
	@Override
	public void generate() {
		// TODO Auto-generated method stub
		System.out.println("Generating TPCDS data");
		
		String cmd = "perl " 
				   + datagen_script + " " 
				   + targetGB + " " 
				   + num_files + " " 
				   + hadoop_slaves + " " 
				   + local_dir + " " 
				   + hdfs_path;

		
		try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd, null, new File(dagegen_script_path));
            
            InputStream stderr =  proc.getErrorStream();
            InputStream stdout = proc.getInputStream();
            
            
            InputStreamReader isr = new InputStreamReader(stderr);
            BufferedReader br = new BufferedReader(isr);
            InputStreamReader isout = new InputStreamReader(stdout);
            BufferedReader br1 = new BufferedReader(isout);
            

            String line = null;
            
            while ( (line = br.readLine()) != null)
            	LOG.error(line);           

            while ( (line = br1.readLine()) != null)
            	LOG.info(line);

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
			
		twitter_mapping.generate();
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
