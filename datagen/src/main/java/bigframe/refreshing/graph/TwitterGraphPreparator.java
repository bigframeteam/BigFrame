package bigframe.refreshing.graph;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.appDomainInfo.BIDomainDataInfo;
import bigframe.datagen.graph.kroneckerGraph.KnonGraphConstants;
import bigframe.datagen.graph.kroneckerGraph.KronGraphGenHadoop;
import bigframe.datagen.graph.kroneckerGraph.KroneckerGraphGen;
import bigframe.refreshing.DataPreparator;

public class TwitterGraphPreparator extends DataPreparator {
	private KroneckerGraphGen graph_gen;
	
	public TwitterGraphPreparator(BigDataInputFormat bigdataIF) {
		super(bigdataIF);
	}

	@Override
	public void prepare() {
		
		// KnonGraphConstants.arScaleVolume2[0]*4 will get us a graph contains about 500,000 user.
		// It will be the unit size for our city.
//		float unit_size = KnonGraphConstants.arScaleVolume2[0];
		
		graph_gen =  new KronGraphGenHadoop(bigdataIF, BIDomainDataInfo.G_STREAM_GB);
		
		graph_gen.setHDFSPATH(bigdataIF.getDataStoredPath().
				get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_GRAPH) + "_update");
		
		graph_gen.generate();
	}

}
