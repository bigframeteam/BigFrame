package bigframe.datagen.factory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenerator;

public abstract class DomainDataInfo {

	protected Set<String> dataVariety;
	protected Set<String> dataVelocity;
	
	protected Map<String, DataGenerator> datagen_map;
	
	protected BigDataInputFormat datainputformat;
	
	public DomainDataInfo(BigDataInputFormat datainputformat) {
		this.datainputformat = datainputformat;
		
		dataVariety = new HashSet<String>();
		dataVelocity = new HashSet<String>();
		datagen_map = new HashMap<String, DataGenerator>();
	}
	
	protected abstract boolean isBigDataIFvalid(); 
	
	public abstract List<DataGenerator> getDataGens();
	
	
	public boolean containDataVariety(String variety) {
		return dataVariety.contains(variety);
	}
	
	public boolean containDataVelocity(String velocity) {
		return dataVelocity.contains(velocity);
	}
}
