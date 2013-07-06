package edu.bigframe.datagen;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.bigframe.util.Config;
import edu.bigframe.util.Constants;

public class DatagenConf extends Config {
	private static final Logger LOG = Logger.getLogger(DatagenConf.class);
	
	protected Set<String> dataVariety;
	protected Integer dataVolume; // Not used in this version
	protected Map<String, Float> dataVelocity;
	protected Map<String, Integer> dataScaleProportions;
	protected Map<String, String> dataStoredPath;

	public DatagenConf() {
		super();

		dataVariety = new HashSet<String>();
		dataVelocity = new HashMap<String, Float>();
		dataScaleProportions = new HashMap<String, Integer>();
		dataStoredPath = new HashMap<String, String>();
	}

	/*
	public void addDataVariety(String type) {
		dataVariety.add(type);
	}

	public void addDataVelocity(String type, Integer rate) {
		dataVelocity.put(type, rate);
	}

	public void addDataScaleFactors(String type, Integer sf) {
		dataScalePortions.put(type, sf);
	}
	*/
	public Set<String> getDataVariety() {
		return dataVariety;
	}

	public Integer getDataVolume() {
		return dataVolume;
	}

	public Map<String, Float> getDataVelocity() {
		return dataVelocity;
	}

	public Map<String, Integer> getDataScaleProportions() {
		return dataScaleProportions;
	}

	public Map<String, String> getDataStoredPath() {
		return dataStoredPath;
	}
	
	/*
	public void setDataVariety(Set<String> dVariety) {
		dataVariety = dVariety;
	}

	public void setDataVolumn(Integer dVolumn) {
		dataVolumn = dVolumn;
	}

	public void setDataVelocity(Map<String, Integer> dVelocity) {
		dataVelocity = dVelocity;
	}

	public void setDataScaleFactors(Map<String, Integer> dSF) {
		dataScalePortions = dSF;
	}
	*/
	@Override
	public void printConf() {
		// TODO Auto-generated method stub
		
		System.out.println("Data generation configuration:");
		System.out.println("Data variety:");
		for (String value : dataVariety) {
			System.out.println(value);
		}
		
		System.out.println("Data velocity:");
		for (Map.Entry<String, Float> entry : dataVelocity.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
		
		System.out.println("Data relative ratio:");
		for (Map.Entry<String, Integer> entry : dataScaleProportions.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
		
		System.out.println("Data hdfs storage path:");
		for (Map.Entry<String, String> entry : dataStoredPath.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}

	@Override
	protected void reloadConf() {
		// TODO Auto-generated method stub

		dataVariety = new HashSet<String>();
		dataVelocity = new HashMap<String, Float>();
		dataScaleProportions = new HashMap<String, Integer>();

		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue();

			
			if (key.equals(Constants.BIGFRAME_DATAVARIETY)) {
				String[] varieties = value.split(",");

				for (String variety : varieties) {
					if (Constants.DATAVARIETY.contains(variety.trim())) {
						dataVariety.add(variety.trim());
					} else
						LOG.warn("Unsupported data type: " + value);
				}
			}

			else if (key.equals(Constants.BIGFRAME_DATAVOLUME)) {
				dataVolume = Constants.DATAVOLUME_MAP.get(value);
			}
			
			else if (Constants.BIGFRAME_DATAVOLUME_PORTION_SET.contains(key)) {
				Integer portion = Integer.parseInt(value);
/*				if (key.equals(Constants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION)) {
					if(!Constants.VALID_RELATIONAL_SF.contains(sf)) {
						LOG.error("Unvalid scale factor for relational data set: "+sf);
						System.exit(-1);
					}
				}
				else if (key.equals(Constants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION)) {
					if(!Constants.VALID_GRAPH_SF.contains(sf)) {
						LOG.error("Unvalid scale factor for graph data set: "+sf);
						System.exit(-1);
					}
				}*/
				
				dataScaleProportions.put(key, portion);
			}

			else if (Constants.BIGFRAME_DATAVELOCITY.contains(key)) {
				dataVelocity.put(key, Float.parseFloat(value));
			}

			else if (Constants.BIGFRAME_DATA_HDFSPATH.contains(key)) {
				dataStoredPath.put(key, value);
			}
		}
	}


}
