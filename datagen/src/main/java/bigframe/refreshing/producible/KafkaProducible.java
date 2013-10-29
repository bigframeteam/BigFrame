package bigframe.refreshing.producible;

public interface KafkaProducible {
	
	public static final String METADATA_BROKER_LIST = "metadata.broker.list";
	public static final String SERIALIZER_CLASS = "seriarlizer.class";
	public static final String PARTITIONER_CALSS = "partitioner.calss";
	public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
	
	public void produce();
	
}
