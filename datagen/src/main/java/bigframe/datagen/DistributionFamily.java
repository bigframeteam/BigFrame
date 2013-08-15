package bigframe.datagen;


/**
 * A class to capture the generation distribution.
 *  
 * @author andy
 *
 */
public abstract class DistributionFamily {
	//private static final Logger LOG = Logger.getLogger(DistributionFamily.class);
	
	// Name of this distribution.
	protected String name;
	
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
