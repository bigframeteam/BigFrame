package bigframe.datagen.graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;



/**
 * Use hadoop to generate the Kronecker Graph.
 * 
 * @author andy
 * 
 */
public class KronGraphGenHadoop extends KroneckerGraphGen {



	public KronGraphGenHadoop(BigDataInputFormat conf, float targetGB) {
		super(conf, targetGB);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void generate() {
		// TODO Auto-generated method stub

		System.out.println("Generating graph data");


		long nodeCount = KroneckerGraphGen.getNodeCount(targetGB);
		steps = (int) Math.log10(nodeCount);
		//float realgraphGB = KroneckerGraphGen.getRealGraphGB(targetGB);
		int nodePerMapper = KnonGraphConstants.NODEGEN_PER_MAPPER;
		int num_Mappers = (int) (nodeCount / nodePerMapper) * (int) (nodeCount / nodePerMapper);



		Configuration mapreduce_config = new Configuration();
		mapreduce_config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
		mapreduce_config.addResource(new Path(conf.getProp().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/mapred-site.xml"));
		mapreduce_config.setInt(KnonGraphConstants.NUM_STEPS, steps);
		mapreduce_config.setInt(KnonGraphConstants.NUM_MAPPERS, num_Mappers);


		try {
			Job job = new Job(mapreduce_config);

			Path outputDir = new Path(hdfs_dir);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setJarByClass(KronGraphGenHadoop.class);
			job.setMapperClass(KronGraphMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(GraphCellInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);


			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	public void setInitialGraph(double[][] graph) {
		// TODO Auto-generated method stub

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
