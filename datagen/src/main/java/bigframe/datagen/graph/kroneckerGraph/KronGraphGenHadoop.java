package bigframe.datagen.graph.kroneckerGraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.GraphCellInputFormat;
import bigframe.util.MapRedConfig;



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

		System.out.println("Generating graph data");


		long nodeCount = KroneckerGraphGen.getNodeCount(targetGB);
		steps = (int) (Math.log(nodeCount)/Math.log(2));
//		float realgraphGB = KroneckerGraphGen.getRealGraphGB(targetGB);
		
		int nodePerMapper = KnonGraphConstants.NODEGEN_PER_MAPPER;
		int num_Mappers = (int) (nodeCount / nodePerMapper) * (int) (nodeCount / nodePerMapper);

		Configuration mapred_config = MapRedConfig.getConfiguration(conf);
		mapred_config.setInt(KnonGraphConstants.NUM_STEPS, steps);
		mapred_config.setInt(KnonGraphConstants.NUM_MAPPERS, num_Mappers);


		try {
			Job job = new Job(mapred_config);

			Path outputDir = new Path(hdfs_path);
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
