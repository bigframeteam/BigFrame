package bigframe.generator.datagen;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import bigframe.generator.BigConfConstants;
import bigframe.generator.util.Config;

/**
 * Class for recording data generation related configuration.
 * 
 * @author andy
 * 
 */
public class DatagenConf extends Config {
	private static final Logger LOG = Logger.getLogger(DatagenConf.class);

	protected String app_domain;
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
	public String getAppDomain() {
		return app_domain;
	}

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


	public float getDataTypeTargetGB(String dataType) {

		float targetGB;

		/**
		 * Get the portion of each data type in terms of the whole volume
		 */
		float sum = 0;
		for (String type : dataVariety) {
			if (type.equals("relational")) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);
			}

			else if (type.equals("graph")) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
			}

			else if (type.equals("nested")) {
				sum += dataScaleProportions
						.get(BigConfConstants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
			}
		}


		if (dataType.equals("relational")) {
			targetGB = dataScaleProportions
					.get(BigConfConstants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION)
					/ sum * dataVolume;
		}

		else if (dataType.equals("graph")) {
			targetGB = dataScaleProportions
					.get(BigConfConstants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION)
					/ sum * dataVolume;
		}

		else if (dataType.equals("nested")) {
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

		System.out.println("Data velocity:");
		for (Map.Entry<String, Float> entry : dataVelocity.entrySet()) {
			if (containDataType(entry.getKey())) {
				System.out.println("\t" + entry.getKey() + ":"
						+ entry.getValue());
			}
		}

		System.out.println("Data proportions:");
		for (Map.Entry<String, Integer> entry : dataScaleProportions.entrySet()) {
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

		dataVariety = new HashSet<String>();
		dataVelocity = new HashMap<String, Float>();
		dataScaleProportions = new HashMap<String, Integer>();

		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue();


			if (key.equals(BigConfConstants.BIGFRAME_DATAVARIETY)) {
				String[] varieties = value.split(",");

				for (String variety : varieties) {
					if (BigConfConstants.DATAVARIETY.contains(variety.trim())) {
						dataVariety.add(variety.trim());
					} else {
						LOG.warn("Unsupported data type: " + value);
					}
				}
			}

			else if (key.equals(BigConfConstants.BIGFRAME_APP_DOMAIN)) {
				app_domain = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_DATAVOLUME)) {
				dataVolume = BigConfConstants.DATAVOLUME_MAP.get(value);
			}

			else if (BigConfConstants.BIGFRAME_DATAVOLUME_PORTION_SET.contains(key)) {
				Integer portion = Integer.parseInt(value);

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
