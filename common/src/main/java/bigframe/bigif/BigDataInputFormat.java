package bigframe.bigif;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import bigframe.util.Config;
import bigframe.util.Constants;

/**
 * Class for recording data generation related configuration.
 * 
 * @author andy
 * 
 */
public class BigDataInputFormat extends Config {
	//private static final Logger LOG = Logger.getLogger(BigDataInputFormat.class);

	protected String app_domain;
	protected Set<String> dataVariety = new HashSet<String>();
	protected Float dataVolume; // In GB
	protected Map<String, Float> dataVelocity = new HashMap<String, Float>();
	protected Map<String, Float> dataScaleProportions = new HashMap<String, Float>();
	protected Map<String, String> dataStoredPath = new HashMap<String, String>();

	public BigDataInputFormat() {
		super();
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
	public String getAppDomain() {
		return app_domain;
	}

	public Set<String> getDataVariety() {
		return dataVariety;
	}

	public Float getDataVolume() {
		return dataVolume;
	}

	public Map<String, Float> getDataVelocity() {
		return dataVelocity;
	}

	public Map<String, Float> getDataScaleProportions() {
		return dataScaleProportions;
	}

	public Map<String, String> getDataStoredPath() {
		return dataStoredPath;
	}


	public float getDataTypeTargetGB(String dataType) {

		float targetGB;

		/**
		 * Get the portion of each data type in terms of the whole volume
		 */
		float sum = 0;
		for (String type : dataVariety) {
			if (type.equals(Constants.RELATIONAL)) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);
			}

			else if (type.equals(Constants.GRAPH)) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
			}

			else if (type.equals(Constants.NESTED)) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
			}
		}


		if (dataType.equals(Constants.RELATIONAL)) {
			targetGB = dataScaleProportions
					.get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION)
					/ sum * dataVolume;
		}

		else if (dataType.equals(Constants.GRAPH)) {
			targetGB = dataScaleProportions
					.get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION)
					/ sum * dataVolume;
		}

		else if (dataType.equals(Constants.NESTED)) {
			targetGB = dataScaleProportions
					.get(BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION)
					/ sum * dataVolume;
		}

		else
			targetGB = 0;

		return targetGB;
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

	private boolean containDataType(String property) {
		boolean flag = false;

		for (String value : dataVariety) {
			if (property.contains(value)) {
				flag = true;
			}
		}

		return flag;
	}

	@Override
	public void printConf() {
		// TODO Auto-generated method stub

		System.out.println("Data generation configuration:");
		System.out.println("Application Domain:");
		System.out.println("\t" + app_domain);


		System.out.println("Data variety:");
		for (String value : dataVariety) {
			System.out.println("\t" + value);

		}
		
		System.out.println("Data volume: " + " around " + dataVolume + "G");
		
		System.out.println("Data velocity:");
		for (Map.Entry<String, Float> entry : dataVelocity.entrySet()) {
			if (containDataType(entry.getKey())) {
				System.out.println("\t" + entry.getKey() + ":"
						+ entry.getValue());
			}
		}

		System.out.println("Data proportions:");
		for (Map.Entry<String, Float> entry : dataScaleProportions.entrySet()) {
			if (containDataType(entry.getKey())) {
				System.out.println("\t" + entry.getKey() + ":"
						+ entry.getValue());
			}
		}


		System.out.println("Data hdfs storage path:");
		for (Map.Entry<String, String> entry : dataStoredPath.entrySet()) {
			if (containDataType(entry.getKey())) {
				System.out.println("\t" + entry.getKey() + ":"
						+ entry.getValue());
			}
		}

	}

	@Override
	protected void reloadConf() {
		// TODO Auto-generated method stub

		// Avoid inconsistent configuration
		dataVariety = new HashSet<String>();
		//dataVelocity = new HashMap<String, Float>();
		//dataScaleProportions = new HashMap<String, Integer>();

		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue().trim();


			if (key.equals(BigConfConstants.BIGFRAME_DATAVARIETY)) {
				String[] varieties = value.split(",");

				for (String variety : varieties) {
//					if (BigConfConstants.DATAVARIETY.contains(variety.trim())) {
						dataVariety.add(variety.trim());
//					} else {
//						LOG.warn("Unsupported data type: " + variety);
//					}
				}
			}

			else if (key.equals(BigConfConstants.BIGFRAME_APP_DOMAIN)) {
				app_domain = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_DATAVOLUME)) {
//				dataVolume = BigConfConstants.DATAVOLUME_MAP.get(value);
				
				dataVolume = Float.parseFloat(value);
			}

			else if (BigConfConstants.BIGFRAME_DATAVOLUME_PORTION_SET.contains(key)) {
				Float portion = Float.parseFloat(value);

				dataScaleProportions.put(key, portion);
			}

			else if (BigConfConstants.BIGFRAME_DATAVELOCITY.contains(key)) {
				dataVelocity.put(key, Float.parseFloat(value));
			}

			else if (BigConfConstants.BIGFRAME_DATA_HDFSPATH.contains(key)) {
				dataStoredPath.put(key, value);
			}

		}
	}


}
