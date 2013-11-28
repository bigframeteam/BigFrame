package bigframe.util.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * The base XML parser. 
 * 
 * @author andy
 *
 * @param <T>
 */
public abstract class XMLBaseParser <T>{
	private static final Log LOG = LogFactory.getLog(XMLBaseParser.class);	
	
	/**
	 * Load from the XML file
	 * 
	 * @param inputFile
	 *            the input file to read from
	 * @return the object loaded from the XML file
	 */
	public T importXML(File inputFile) {

		InputStream in = null;
		T t = null;
		try {
			in = new FileInputStream(inputFile);
			t = importXML(in);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
			}
		}

		return t;
	}

	/**
	 * Load from the XML representation
	 * 
	 * @param in
	 *            the input stream to read from
	 * @return the object loaded from the XML stream
	 */
	public T importXML(InputStream in) {

		// Parse the input stream
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringComments(true);
		Document doc = null;
		T t = null;
		try {
			doc = dbf.newDocumentBuilder().parse(in);
			t = importXML(doc);
		} catch (ParserConfigurationException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} catch (SAXException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		} catch (IOException e) {
			LOG.error("Unable to import XML file", e);
			t = null;
		}

		return t;
	}

	/**
	 * Create an XML DOM representing the input object and write out to the
	 * provided output file
	 * 
	 * @param object
	 *            the object to export in the XML file
	 * @param outFile
	 *            the output file
	 */
	public void exportXML(T object, File outFile) {

		PrintStream out = null;
		try {
			out = new PrintStream(outFile);
			exportXML(object, out);
		} catch (FileNotFoundException e) {
			LOG.error("Unable to export XML file", e);
		} finally {
			if (out != null)
				out.close();
		}
	}

	/**
	 * Create an XML DOM representing the object and write out to the provided
	 * output stream
	 * 
	 * @param object
	 *            the object to export in the XML stream
	 * @param out
	 *            the output stream
	 */
	public void exportXML(T object, PrintStream out) {

		Document doc = null;
		try {
			// Workaround to get the right FactoryBuilder
			System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
					"com.sun.org.apache.xerces."
							+ "internal.jaxp.DocumentBuilderFactoryImpl");
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
					.newDocument();
		} catch (ParserConfigurationException e) {
			LOG.error("Unable to export XML file", e);
		}

		// Create the dom
		exportXML(object, doc);

		// Write out the XML output
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(out);
		try {
			Transformer transformer = TransformerFactory.newInstance()
					.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(source, result);
		} catch (TransformerConfigurationException e) {
			LOG.error("Unable to export XML file", e);
		} catch (TransformerException e) {
			LOG.error("Unable to export XML file", e);
		}
	}

	/* ***************************************************************
	 * PROTECTED ABSTRACT METHODS
	 * ***************************************************************
	 */

	/**
	 * Import the XML representation of the object from the XML Document
	 * 
	 * @param doc
	 *            the XML document
	 * @return the imported object
	 */
	protected abstract T importXML(Document doc);

	/**
	 * Export the XML representation of the object in the XML Document
	 * 
	 * @param object
	 *            the object to export
	 * @param doc
	 *            the XML Document
	 */
	protected abstract void exportXML(T object, Document doc);
}
