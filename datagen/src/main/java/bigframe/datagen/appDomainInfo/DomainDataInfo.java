package bigframe.datagen.appDomainInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenerator;
import bigframe.refreshing.DataPreparator;
import bigframe.refreshing.DataProducer;

/**
 * A class encapsulates all the data generation restraint for 
 * an application domain. 
 * Every extended application domain needs to inherit this class.	 
 * 
 * @author andy
 *
 */
public abstract class DomainDataInfo {

	protected Set<String> supported_dataVariety;
	protected Set<String> supported_dataVelocity;
	
	protected Map<String, DataGenerator> datagen_map;
	protected Map<String, DataPreparator> dataPrep_map;
	protected Map<String, DataProducer> dataProd_map;
	
	protected BigDataInputFormat datainputformat;
	
	public DomainDataInfo(BigDataInputFormat datainputformat) {
		this.datainputformat = datainputformat;
		
		supported_dataVariety = new HashSet<String>();
		supported_dataVelocity = new HashSet<String>();
		datagen_map = new HashMap<String, DataGenerator>();
		dataPrep_map = new HashMap<String, DataPreparator>();
		dataProd_map = new HashMap<String, DataProducer>(); 
	}
	
	protected abstract boolean isBigDataIFvalid(); 
	
	public abstract List<DataGenerator> getDataGens();
	
	
	public abstract List<DataPreparator> getDataPreps();
	
	public abstract List<DataProducer> getDataProds();
	
	public boolean containDataVariety(String variety) {
		return supported_dataVariety.contains(variety);
	}
	
	public boolean containDataVelocity(String velocity) {
		return supported_dataVelocity.contains(velocity);
	}
}
