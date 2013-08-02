package bigframe.generator.datagen.relational;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import bigframe.generator.BigConfConstants;
import bigframe.generator.datagen.DataGenerator;
import bigframe.generator.datagen.DatagenConf;


public class TpcdsDataGenNaive extends TpcdsDataGen {
	private static final Logger LOG = Logger.getLogger(DataGenerator.class);
	
	private int num_files;
	private String hdfs_dir;
	private String local_dir;
	private String datagen_script;
	private String hadoop_slaves;
	private String dagegen_script_path;
	
	public TpcdsDataGenNaive(DatagenConf conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		
		num_files = (int) (targetGB/2);
		if (num_files < 2) {
			num_files = 2;
		}

		hdfs_dir = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL);
		local_dir = conf.getProp().get(BigConfConstants.BIGFRAME_TPCDS_LOCAL);
		datagen_script = conf.getProp().get(BigConfConstants.BIGFRAME_TPCDS_SCRIPT);
		hadoop_slaves = conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_SLAVE);
		
		dagegen_script_path = (new File(datagen_script)).getParentFile().getAbsolutePath();
	}

	@Override
	public void generate() {
		// TODO Auto-generated method stub
		System.out.println("Generating TPCDS data");
		
		String cmd = "perl " 
				   + datagen_script + " " 
				   + (int) targetGB + " " 
				   + num_files + " " 
				   + hadoop_slaves + " " 
				   + local_dir + " " 
				   + hdfs_dir;

		
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
		
		RelationalDataGen twitter_mapping = new TwitterMappingGenNaive(conf,targetGB);
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
