package bigframe.datagen.relational.tpcds;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Some constants used by tpcds generator.
 * 
 * @author andy
 *
 */
public class TpcdsConstants {

	public static String TPCDS_BEGINDATE = "1900-01-01";
	public static String TPCDS_ENDDATE = "2100-01-01";
	
	public static DateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd");
}
