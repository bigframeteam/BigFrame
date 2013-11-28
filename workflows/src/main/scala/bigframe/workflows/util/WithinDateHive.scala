package bigframe.workflows.util

import org.apache.hadoop.hive.ql.exec.UDF

class WithinDateHive extends UDF {

	val dataUtil = new DateUtils
	
	def evaluate(created_at: String, start_date: String, 
			end_date: String): Boolean = dataUtil.isDateWithin(created_at, start_date, end_date)

}