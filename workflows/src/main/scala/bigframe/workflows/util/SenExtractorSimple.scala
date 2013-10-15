package bigframe.workflows.util

class SenExtractorSimple extends SentimentExtractor {
	
	private val dict_map: Map[String, Float] =  {
		val dict_in = getClass.getResourceAsStream("/AFINN-111.txt")
		
		val br = new java.io.BufferedReader(new java.io.InputStreamReader(dict_in))
		
		var dict_map = Map[String, Float]()
		
		var line = br.readLine()
		
		while(line != null) {
			if(line.split("\t").length == 2 && line.split("\t")(1) != "")
				dict_map +=  (line.split("\t")(0)-> line.split("\t")(1).toInt)
			line = br.readLine()
		}
		
		dict_map
	}
	
	override def getSentiment(text: String): Float = {
		var score = 0f
		
		for(word <- text.split(" +")) {
			score += dict_map.getOrElse(word, 0f) 
		}
			
		return score
	}
}