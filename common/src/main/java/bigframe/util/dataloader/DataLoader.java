package bigframe.util.dataloader;

import bigframe.bigif.WorkflowInputFormat;


/**
 * An abstract class for data loading utility.
 * The real data loading between different storage engines is
 * Done by the concrete class.
 *   
 * @author andy
 *
 */
public abstract class DataLoader {

	protected WorkflowInputFormat workIF;
	
	public DataLoader(WorkflowInputFormat workIF) {
		this.workIF = workIF;
	}
	
	/**
	 * The abstract method for data loading.
	 * 
	 * @return true if successful; else false 
	 */
	public abstract boolean load();
}
