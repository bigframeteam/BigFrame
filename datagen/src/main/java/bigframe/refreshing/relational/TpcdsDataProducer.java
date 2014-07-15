package bigframe.refreshing.relational;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.refreshing.DataProducer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class TpcdsDataProducer extends DataProducer {

	private List<String> s_purchase = new ArrayList<String>();
	private List<String> s_purchase_lineitem = new ArrayList<String>();
	
	private List<String> s_web_order = new ArrayList<String>();
	private List<String> s_web_order_lineitem = new ArrayList<String>();
	
	private List<String> s_catalog_order = new ArrayList<String>();
	private List<String> s_catalog_order_lineitem = new ArrayList<String>();

	private SimpleDateFormat twitterDateFormat = new SimpleDateFormat(
			"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			
	public TpcdsDataProducer(BigDataInputFormat bigdataIF) {
		super(bigdataIF);
	}

	
	class PurchaseProducer implements Runnable{
		
		ProducerConfig config;
		
		private BigDataInputFormat bigdataIF;
		
		public PurchaseProducer(BigDataInputFormat bigdataIF, ProducerConfig config) {
			
			this.bigdataIF = bigdataIF;
			this.config = config;
			
		}

		@Override
		public void run() {
			
			Producer<String, String> producer = new Producer<String, String>(config);
			
	        /**
	         * Every purchase contains 12 items
	         * Every web order contains 12 items
	         * Every catalog order contains 9 items
	         */
			while(true) {
//				long start_time = System.currentTimeMillis();
				for(int i = 0; i < s_purchase.size(); i++) {
					
					String time_stamp = twitterDateFormat.format(System.currentTimeMillis());
					
					KeyedMessage<String, String> data1 = new KeyedMessage<String, String>("s_purchase", time_stamp + "|" + s_purchase.get(i));
					producer.send(data1);
					
					int count = 0;
					for(int j = i * 12; j < s_purchase_lineitem.size();j++) {						
						KeyedMessage<String, String> data2 = new KeyedMessage<String, String>("s_purchase_lineitem", time_stamp + "|" + 
								s_purchase_lineitem.get(j));
						producer.send(data2);
						
						count++;						
						if(count >= 12)
							break;
					}
					
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
//				long stop_time = System.currentTimeMillis();
//
//				try {
//					
//					if(stop_time-start_time <= 1000)					
//						Thread.sleep(1000 - (stop_time-start_time));
//					
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		}
		
	}
	
	
	class CatalogOrderProducer implements Runnable{
		
		
		ProducerConfig config;
		
		private BigDataInputFormat bigdataIF;
		
		public CatalogOrderProducer(BigDataInputFormat bigdataIF,ProducerConfig config ) {
			
			this.bigdataIF = bigdataIF;		
			this.config = config;
		}

		@Override
		public void run() {
			
			Producer<String, String> producer = new Producer<String, String>(config);
			
	        /**
	         * Every purchase contains 12 items
	         * Every web order contains 12 items
	         * Every catalog order contains 9 items
	         */
			while(true) {
				
				while(true) {
//					long start_time = System.currentTimeMillis();
					for(int i = 0; i < s_purchase.size(); i++) {
						String time_stamp = twitterDateFormat.format(System.currentTimeMillis());
						
						KeyedMessage<String, String> data1 = new KeyedMessage<String, String>("s_catalog_order", time_stamp + "|" + s_catalog_order.get(i));
						producer.send(data1);
						
						int count = 0;
						for(int j = i * 9; j < s_purchase_lineitem.size();j++) {						
							KeyedMessage<String, String> data2 = new KeyedMessage<String, String>("s_catalog_order_lineitem", time_stamp + "|" + 
										s_catalog_order_lineitem.get(j));
							producer.send(data2);
							
							count++;						
							if(count >= 9)
								break;
						}
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
//					long stop_time = System.currentTimeMillis();
//
//					try {
//						
//						if(stop_time-start_time <= 1000)					
//							Thread.sleep(1000 - (stop_time-start_time));
//						
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
				}
			}
		}
		
	}
	
	class WebOrderProducer implements Runnable{

		ProducerConfig config;
		
		private BigDataInputFormat bigdataIF;
		
		public WebOrderProducer(BigDataInputFormat bigdataIF, ProducerConfig config ) {
			
			this.bigdataIF = bigdataIF;
			this.config = config;
			
		}

		@Override
		public void run() {
			Producer<String, String> producer = new Producer<String, String>(config);
			
	        /**
	         * Every purchase contains 12 items
	         * Every web order contains 12 items
	         * Every catalog order contains 9 items
	         */
			while(true) {
				
				while(true) {
//					long start_time = System.currentTimeMillis();
					for(int i = 0; i < s_purchase.size(); i++) {
						String time_stamp = twitterDateFormat.format(System.currentTimeMillis());
						
						KeyedMessage<String, String> data1 = new KeyedMessage<String, String>("s_web_order", time_stamp + "|" + 
						s_web_order.get(i));
						producer.send(data1);
						
						int count = 0;
						for(int j = i * 12; j < s_purchase_lineitem.size();j++) {						
							KeyedMessage<String, String> data2 = new KeyedMessage<String, String>("s_web_order_lineitem", time_stamp + "|" + 
									s_web_order_lineitem.get(j));
							producer.send(data2);
							
							count++;						
							if(count >= 12)
								break;
						}
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
//					long stop_time = System.currentTimeMillis();
//
//					try {
//						
//						if(stop_time-start_time <= 1000)					
//							Thread.sleep(1000 - (stop_time-start_time));
//						
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
				}
			}
		}
		
	}
	
	/**
	 * 
	 */
	@Override
	public void init() {
		
		/**
		 * Loading the prepared data
		 */
		
		System.out.println("Loading the prepared data for TPCDS...");
		
		Configuration mapred_config = new Configuration();
		mapred_config.addResource(new Path(bigdataIF.get().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/core-site.xml"));
		mapred_config.addResource(new Path(bigdataIF.get().get(BigConfConstants.BIGFRAME_HADOOP_HOME)+"/conf/mapred-site.xml"));	
	
		try {
			FileSystem fs;
			fs = FileSystem.get(mapred_config);
			
			Path stream_data_path = new Path(bigdataIF.get().get(BigConfConstants.BIGFRAME_DATA_HDFSPATH_RELATIONAL) + "_streaming");
			
			if(!fs.exists(stream_data_path)) {
				System.out.println("Target path: " + stream_data_path + " not exists!");
				System.out.println("Streaming data set has not prepared, please run \"datagen -mode prepare\" first!");
				System.exit(-1);
			}
			
			FileStatus[] status1 = fs.listStatus(stream_data_path);
			
			for (FileStatus stat1 : status1){
				if(stat1.getPath().toString().contains("purchase") ) {		
					FileStatus[] status2 = fs.listStatus(stat1.getPath());
					
					if (stat1.getPath().toString().contains("purchase_lineitem")) {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_purchase_lineitem.add(line);
							}
						}
					}
					else {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_purchase.add(line);
							}
						}
					}

				}
				
				else if(stat1.getPath().toString().contains("web_order") ) {		
					FileStatus[] status2 = fs.listStatus(stat1.getPath());
					
					if (stat1.getPath().toString().contains("web_order_lineitem")) {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_web_order_lineitem.add(line);
							}
						}
					}
					else {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_web_order.add(line);
							}
						}
					}

				}
				
				else if(stat1.getPath().toString().contains("catalog_order") ) {		
					FileStatus[] status2 = fs.listStatus(stat1.getPath());
					
					if (stat1.getPath().toString().contains("catalog_order_lineitem")) {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_catalog_order_lineitem.add(line);
							}
						}
					}
					else {
						for(FileStatus stat2 : status2) {
							BufferedReader in = new BufferedReader(new InputStreamReader(
									fs.open(stat2.getPath())));
							
							String line;
							while ((line = in.readLine()) != null) {
								s_catalog_order.add(line);
							}
						}
					}

				}
	
			}
			
		System.out.println("Finish Loading!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *
	 */
	@Override
	public void produce() {
		
		ExecutorService pool = Executors.newFixedThreadPool(3);
		
		PurchaseProducer p_producer = new PurchaseProducer(bigdataIF, config);
		CatalogOrderProducer c_producer = new CatalogOrderProducer(bigdataIF, config);
		WebOrderProducer w_producer = new WebOrderProducer(bigdataIF, config);
		
		pool.submit(p_producer);
		pool.submit(w_producer);
		pool.submit(c_producer);
	}

}
