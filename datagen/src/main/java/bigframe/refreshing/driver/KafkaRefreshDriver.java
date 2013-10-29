package bigframe.refreshing.driver;

import java.util.List;

import bigframe.bigif.BigDataInputFormat;
import bigframe.refreshing.DataPreparator;
import bigframe.refreshing.DataPreparatorFactory;
import bigframe.refreshing.RefreshDriver;



/**
 * Using Apache Kafka as the distributed data producer.
 *  
 * @author andy
 *
 */
public class KafkaRefreshDriver extends RefreshDriver {

	public KafkaRefreshDriver(BigDataInputFormat bigdataIF) {
		super(bigdataIF);
	}

	@Override
	public void refresh() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare() {
		List<DataPreparator> preparators = DataPreparatorFactory.createPreparators(bigdataIF);
		
		for(DataPreparator prep : preparators) {
			prep.prepare();
		}
		
		
		
	}

}
