package bigframe.util;

import java.util.Map;

import bigframe.bigif.BigConfConstants;

public class SparkConfig extends Config {

	private String SHARK_HOME = "";
	private boolean SHARK_RC = true;
	private boolean SHARK_SNAPPY = true;
	
	private String SPARK_HOME = "";
	private String SPARK_MASTER = "";
	private String SPARK_LOCAL_DIR = "";
	private Boolean SPARK_USE_BAGEL = true;
	private Integer SPARK_DOP = 8;
	private Boolean SPARK_COMPRESS_MEMORY = false;
	private Float SPARK_MEMORY_FRACTION = 0.66f;
	private Boolean SPARK_OPTIMIZE_MEMORY = true;
	
	public SparkConfig() {
		// TODO Auto-generated constructor stub
	}

	public String getSharkHome() {
		return SHARK_HOME;
	}
	
	public boolean getSharkRC() {
		return SHARK_RC;
	}
	
	public boolean getSharkSnappy() {
		return SHARK_SNAPPY;
	}
	
	public String getSparkHome() {
		return SPARK_HOME;
	}
	
	public String getSparkMaster() {
		return SPARK_MASTER;
	}
	
	public String getSparkLocalDir() {
		return SPARK_LOCAL_DIR;
	}

	public Boolean getSparkUseBagel() {
		return SPARK_USE_BAGEL;
	}

	public Integer getSparkDoP() {
		return SPARK_DOP;
	}

	public Boolean getSparkCompressMemory() {
		return SPARK_COMPRESS_MEMORY;
	}

	public Float getSparkMemoryFraction() {
		return SPARK_MEMORY_FRACTION;
	}

	public Boolean getSparkOptimizeMemory() {
		return SPARK_OPTIMIZE_MEMORY;
	}
	
	@Override
	protected void reloadConf() {
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue().trim();
			
			if (key.equals(BigConfConstants.BIGFRAME_SHARK_HOME)) {
				SHARK_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_SHARK_RC)) {
				SHARK_RC = Boolean.valueOf(value);
			}
			
			
			else if (key.equals(BigConfConstants.BIGFRAME_SHARK_ENABLE_SNAPPY)) {
				SHARK_SNAPPY = Boolean.valueOf(value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_HOME)) {
				SPARK_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_MASTER)) {
				SPARK_MASTER = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_LOCAL_DIR)) {
				SPARK_LOCAL_DIR = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_USE_BAGEL)) {
				SPARK_USE_BAGEL = Boolean.valueOf(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_DOP)) {
				SPARK_DOP = Integer.parseInt(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_COMPRESS_MEMORY)) {
				SPARK_COMPRESS_MEMORY = Boolean.valueOf(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_MEMORY_FRACTION)) {
				SPARK_MEMORY_FRACTION = Float.parseFloat(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_OPTIMIZE_MEMORY)) {
				SPARK_OPTIMIZE_MEMORY = Boolean.valueOf(value);
			}
		}
		
	}

	@Override
	public void printConf() {
		// TODO Auto-generated method stub
		
	}
	

}
