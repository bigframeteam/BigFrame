package bigframe.workflows.util


/**
 * A factory class to create sentiment analyze instance.
 * 
 * @author andy
 */
object SenExtractorFactory {
	
	def getSenAnalyze(name: SenExtractorEnum) = 
		if(name == SenExtractorEnum.SIMPLE)  new SenExtractorSimple() else null 
	
}