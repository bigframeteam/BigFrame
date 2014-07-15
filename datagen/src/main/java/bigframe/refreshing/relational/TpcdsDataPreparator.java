package bigframe.refreshing.relational;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.appDomainInfo.BIDomainDataInfo;
import bigframe.datagen.relational.tpcds.TpcdsDataGenNaive;
import bigframe.refreshing.DataPreparator;

public class TpcdsDataPreparator extends DataPreparator {
	private static final Logger LOG = Logger.getLogger(TpcdsDataPreparator.class);
	
	private int num_files;
	private String hdfs_dir;
	private String local_dir;
	private String dataupdate_script;
	private String hadoop_slaves;
	private String dageupdate_script_path;
	
	/**
	 * This is the unit for one city.
	 */
	float targetGB = BIDomainDataInfo.R_STREAM_GB;
	
	public TpcdsDataPreparator(BigDataInputFormat bigdataIF) {
		super(bigdataIF);
	}
	
	private void genStreamData() {
		num_files = (int) (targetGB/2) + 1;
		if (num_files < 2) {
			num_files = 2;
		}
		
		hdfs_dir = bigdataIF.getDataStoredPath().
				get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "_streaming";
		local_dir = bigdataIF.get().get(BigConfConstants.BIGFRAME_TPCDS_LOCAL);
		dataupdate_script = bigdataIF.get().get(BigConfConstants.BIGFRAME_TPCDS_UPDATESCRIPT);
		hadoop_slaves = bigdataIF.get().get(BigConfConstants.BIGFRAME_HADOOP_SLAVE);
		
		dageupdate_script_path = (new File(dataupdate_script)).getParentFile().getAbsolutePath();
		
		
		/**
		 * It will generate an update set around 10% * targetGB.
		 */
		String cmd = "perl " 
				   + dataupdate_script + " " 
				   + targetGB + " " 
				   + num_files + " " 
				   + hadoop_slaves + " " 
				   + local_dir + " " 
				   + hdfs_dir + " "
				   + 1;

		
		try {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd, null, new File(dageupdate_script_path));
            
            InputStream stdout = proc.getInputStream();           

            InputStreamReader isout = new InputStreamReader(stdout);
            BufferedReader br1 = new BufferedReader(isout);        

            String line = null; 

            while ( (line = br1.readLine()) != null)
            	LOG.info(line);

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void prepare() {

		System.out.println("Preparing TPCDS update data");
		
		/**
		 * FIXME
		 * 
		 * The twitter graph generate wrong mapping, since the graph size is fixed now. 
		 */
		TpcdsDataGenNaive tpcds_gen = new TpcdsDataGenNaive(bigdataIF, targetGB);
		tpcds_gen.setHDFSPATH(bigdataIF.getDataStoredPath().
				get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "_update");
		
		tpcds_gen.generate();
		
		genStreamData();
		
	}

}
