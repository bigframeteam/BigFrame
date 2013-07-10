package bigframe;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import bigframe.datagen.DatagenConf;
import bigframe.querygen.QuerygenConf;
import bigframe.util.XMLBaseParser;




public class XMLBenchmarkSpecsParser extends XMLBaseParser<BenchmarkConf> {
	
	private static final String CONF = "configuration";
	private static final String PROP = "property";
	private static final String NAME = "name";
	private static final String VALUE = "value";


	@Override
	protected BenchmarkConf importXML(Document doc) {
		// TODO Auto-generated method stub
		
		Element root = doc.getDocumentElement();
		if (!CONF.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <configuration>");
		
		DatagenConf datagenConf = new DatagenConf();
		QuerygenConf querygenConf = new QuerygenConf();
		
		NodeList properties = root.getElementsByTagName(PROP);
		
		Map<String, String> prop_map = new HashMap<String,String>();
		for (int i = 0; i < properties.getLength(); ++i) {
			Node prop = properties.item(i);
			if (prop.getNodeType() == Node.ELEMENT_NODE) {
				Element element  = (Element) prop;
				prop_map.put(getValue(NAME, element), getValue(VALUE, element));
			}
			
		}
		
		datagenConf.set(prop_map);
		querygenConf.set(prop_map);
		
		BenchmarkConf benchmarkConf = new BenchmarkConf(datagenConf, querygenConf);
		return benchmarkConf;
	}

	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}

	
	@Override
	protected void exportXML(BenchmarkConf object, Document doc) {
		// TODO Auto-generated method stub
		
	}

}
