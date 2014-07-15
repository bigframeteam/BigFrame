package bigframe.refreshing;

import java.util.Properties;
import java.util.concurrent.Callable;

import kafka.producer.ProducerConfig;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;

public abstract class DataProducer implements Callable<Object>{
	protected BigDataInputFormat bigdataIF;
	protected float stream_sf = 1;
	
	protected ProducerConfig config;
	
	public DataProducer(BigDataInputFormat bigdataIF) {
		this.bigdataIF = bigdataIF;
		
		Properties props = new Properties();
		
		String kafka_broker_list = bigdataIF.get().get(BigConfConstants.BIGFRAME_KAFKA_BROKER_LIST);
		
		props.put("metadata.broker.list", kafka_broker_list);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "2");
		//props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");
		 
		
		config = new ProducerConfig(props);
	}

	public abstract void init();
	
	public abstract void produce();
	
	public Object call() throws Exception {
		produce();
		return null;
	}
	
	public void setStreamScaleFactor(float sf) {
		stream_sf = sf;
	}
}
