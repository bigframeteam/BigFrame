package bigframe.datagen.factory;

import java.util.List;


import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;

import bigframe.datagen.DataGenerator;


public class DatagenFactory {
	//private static final Logger LOG = Logger.getLogger(DatagenFactory.class);
	private BigDataInputFormat datainputformat;

	public DatagenFactory(BigDataInputFormat datainputformat) {
		this.datainputformat = datainputformat;
	}


	/**Create the set of data generator based on the data variety user specified.
	 * 
	 * @return List of data generator
	 */
	public List<DataGenerator> createGenerators() {
		String app_domain = datainputformat.getAppDomain();
		
		DomainDataInfo dataInfo;
		if (app_domain.equals(BigConfConstants.APPLICATION_BI)) {
			dataInfo = new BIDomainDataInfo(datainputformat);
		}
		else
			return null;

		return dataInfo.getDataGens();
	}
}
