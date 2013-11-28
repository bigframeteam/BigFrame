package bigframe.datagen;

import java.util.List;


import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;

import bigframe.datagen.appDomainInfo.BIDomainDataInfo;
import bigframe.datagen.appDomainInfo.DomainDataInfo;

/**
 * A factory class to collect all the data generator for a 
 * specific application domain.
 * 
 * @author andy
 *
 */
public class DatagenFactory {

	/**Create the set of data generator based on the data variety user specified.
	 * 
	 * @return List of data generator
	 */
	public static List<DataGenerator> createGenerators(BigDataInputFormat datainputformat) {
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
