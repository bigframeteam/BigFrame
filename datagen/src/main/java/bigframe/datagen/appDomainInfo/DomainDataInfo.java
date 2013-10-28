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

	protected Set<String> dataVariety;
	protected Set<String> dataVelocity;
	
	protected Map<String, DataGenerator> datagen_map;
	protected Map<String, DataPreparator> dataPrep_map;
	protected Map<String, DataProducer> dataProd_map;
	
	protected BigDataInputFormat datainputformat;
	
	public DomainDataInfo(BigDataInputFormat datainputformat) {
		this.datainputformat = datainputformat;
		
		dataVariety = new HashSet<String>();
		dataVelocity = new HashSet<String>();
		datagen_map = new HashMap<String, DataGenerator>();
		dataPrep_map = new HashMap<String, DataPreparator>();
		dataProd_map = new HashMap<String, DataProducer>(); 
	}
	
	protected abstract boolean isBigDataIFvalid(); 
	
	public  List<DataGenerator> getDataGens() {
		if (!isBigDataIFvalid()) {
			return null;
		}
		
		List<DataGenerator> datagen_list = new LinkedList<DataGenerator>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			datagen_list.add(datagen_map.get(variety));
		}
		return datagen_list;
	}
	
	
	public List<DataPreparator> getDataPreps() {
		if (!isBigDataIFvalid()) {
			return null;
		}
		
		List<DataPreparator> dataPrep_list = new LinkedList<DataPreparator>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			dataPrep_list.add(dataPrep_map.get(variety));
		}
		return dataPrep_list;
	}
	
	public List<DataProducer> getDataProds() {
		if (!isBigDataIFvalid()) {
			return null;
		}
		
		List<DataProducer> dataPrep_list = new LinkedList<DataProducer>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			dataPrep_list.add(dataProd_map.get(variety));
		}
		return dataPrep_list;
	}
	
	public boolean containDataVariety(String variety) {
		return dataVariety.contains(variety);
	}
	
	public boolean containDataVelocity(String velocity) {
		return dataVelocity.contains(velocity);
	}
}
