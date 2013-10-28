package bigframe.refreshing;

import java.util.List;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.appDomainInfo.BIDomainDataInfo;
import bigframe.datagen.appDomainInfo.DomainDataInfo;

public class DataPreparatorFactory {

	public static List<DataPreparator> createPreparators(BigDataInputFormat bigdataIF) {	
		String app_domain = bigdataIF.getAppDomain();
		
		DomainDataInfo dataInfo;
		if (app_domain.equals(BigConfConstants.APPLICATION_BI)) {
			dataInfo = new BIDomainDataInfo(bigdataIF);
		}
		else
			return null;

		return dataInfo.getDataPreps();

	}
}
