package bigframe.bigif;

import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import bigframe.util.XMLBaseParser;



/**
 * XML Parser for parsing bigframe specification.
 * 
 * @author andy
 * 
 */
public class XMLBigFrameInputParser extends XMLBaseParser<BigFrameInputFormat> {

	private static final String CONF = "configuration";
	private static final String PROP = "property";
	private static final String NAME = "name";
	private static final String VALUE = "value";


	@Override
	protected BigFrameInputFormat importXML(Document doc) {
		// TODO Auto-generated method stub

		Element root = doc.getDocumentElement();
		if (!CONF.equals(root.getTagName()))
			throw new RuntimeException(
					"ERROR: Bad XML File: top-level element not <configuration>");

		BigDataInputFormat datagenConf = new BigDataInputFormat();
		BigQueryInputFormat querygenConf = new BigQueryInputFormat();

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

		BigFrameInputFormat benchmarkConf = new BigFrameInputFormat(datagenConf, querygenConf);
		return benchmarkConf;
	}

	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
		Node node = nodes.item(0);
		return node.getNodeValue();
	}


	@Override
	protected void exportXML(BigFrameInputFormat object, Document doc) {
		// TODO Auto-generated method stub

	}

}
