package bigframe.datagen.relational.twittermapping;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.datagen.relational.tpcds.CollectTPCDSstat;
import bigframe.datagen.relational.tpcds.CollectTPCDSstatNaive;
import bigframe.datagen.util.RandomSeeds;

import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;

/**
 * A single machine implementation for the twitter mapping generation.
 * @author andy
 *
 */
public class TwitterMappingGenNaive extends TwitterMappingGen {
	
	private static final Logger LOG = Logger.getLogger(TwitterMappingGenNaive.class);
	
	private float tpcds_proportion;
	private float twitter_graph_proportion;
	
	private int num_customer;
	private long num_twitter_user;
	
	private String hdfs_path; 
		
	public void setHDFS_PATH(String path) {
		hdfs_path = path;
	}
	
	public TwitterMappingGenNaive(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
		tpcds_proportion = conf.getDataScaleProportions().get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);
		twitter_graph_proportion = conf.getDataScaleProportions().get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
		
		float graph_targetGB = twitter_graph_proportion /tpcds_proportion * targetGB;
		
		CollectTPCDSstat tpcds_stat_collecter = new CollectTPCDSstatNaive();
		num_customer = (int) (tpcds_stat_collecter.getNumOfCustomer((int)targetGB));
		num_twitter_user = (long) KroneckerGraphGen.getNodeCount(graph_targetGB);
		
		//System.out.println("Num of customer:"+num_customer);
		//System.out.println("Num of customer:"+num_twitter_user);
		//num_twitter_user = Constants.GRAPH_SF_MAP.get(twitter_graph_proportion);
		
		if(!valid()){
			LOG.error("Set a smaller proportion for relational or set a larger proportion for graph!");
			System.exit(-1);
		}
		
		hdfs_path = conf.getDataStoredPath().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "/" + "twitter_mapping";
	}

	/**
	 * Check if the number of twitter user is more than the number of customer.
	 * Return false if the latter one is larger.
	 * 
	 * @return if num_twitter_user <num_customer
	 */
	private boolean valid() {
		if (num_twitter_user <num_customer)
			return false;
		else 
			return true;
	}
	
	/**
	 * Do random shuffling for an array.
	 * @param ar
	 */
	private void shuffleArray(long[] ar)
	{
	    Random rnd = new Random();
	    for (int i = ar.length - 1; i >= 0; i--)
	    {
	      int index = rnd.nextInt(i + 1);
	      // Simple swap
	      long a = ar[index];
	      ar[index] = ar[i];
	      ar[i] = a;
	    }
	}
	
	
	@Override
	public void generate() {
		LOG.info("Generating Twitter Mapping data");
		
		// TODO Auto-generated method stub
		try {
			Path path = new Path(hdfs_path);
			Configuration config = new Configuration();
			config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
			FileSystem fileSystem = FileSystem.get(config);
			
			if (!fileSystem.exists(path))
				fileSystem.mkdirs(path);
			else {
				System.out.println("Directory "+ path+ " is existed!");
				System.exit(-1);
			}
			
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(path+"/twitter_mapping.dat"), true)));

			
			long [] customers = new long[num_customer/2];
			long [] twitter_accounts = new long[num_customer/2];
			
			//Initialize seed 
			RandomEngine twister = new MersenneTwister(RandomSeeds.SEEDS_TABLE[0]);
			
			/**
			 * Sample a set of customer and twitter account respectively.
			 * The returned sample is sorted by values.
			 */
			RandomSampler.sample(num_customer/2, num_customer, num_customer/2, 1,customers , 0, twister);
			RandomSampler.sample(num_customer/2, num_twitter_user, num_customer/2, 1,twitter_accounts , 0, twister);
			
			/**
			 * We need to shuffle the set of twitter account such that
			 * the mapping is random. 
			 */
			shuffleArray(twitter_accounts);
			for (int i = 0; i < num_customer/2; i++) {
				bufferedWriter.write(customers[i]+"|"+twitter_accounts[i]);
				bufferedWriter.newLine();
			}
			bufferedWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
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
