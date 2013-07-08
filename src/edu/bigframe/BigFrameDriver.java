package edu.bigframe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.bigframe.datagen.DataGenerator;
import edu.bigframe.datagen.DatagenFactory;
import edu.bigframe.datagen.nested.RawTweetGenNaive;
import edu.bigframe.datagen.relational.CollectTPCDSstat;
import edu.bigframe.datagen.relational.CollectTPCDSstatNaive;
import edu.bigframe.datagen.relational.PromotionInfo;
import edu.bigframe.datagen.relational.TwitterMappingGenNaive;
import edu.bigframe.util.Constants;


/**
 * The entrance of BigFrame.
 * @author andy
 *
 */

public class BigFrameDriver {



	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(BigFrameDriver.class);

	
	
	// Main parsing options
	private static String MODE = "mode";
	private static String CONF = "conf";
	private static String HELP = "help";
	private static String MODE_DATAGEN = "datagen";
	private static String MODE_REFRESH = "refresh";

	


	
	private static void printUsage(PrintStream out) {

		out.println();
		out.println("Usage: bigframe");
		out.println(" -mode								[required] Currently, these modes are supportted:");
		out.println("									[datagen, refresh]");
		out.println(" -conf <config file>				Benchmark configuration file");
		out.println(" -help								Print this usage");
		out.println("");

	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("static-access")
	private static Options buildOptions() {

		// Build the options
		Option modeOption = OptionBuilder.withArgName(MODE).hasArg()
				.withDescription("Execution mode options").create(MODE);

		Option confOption = OptionBuilder.withArgName(CONF).hasArg()
				.withDescription("The benchmark configuration file").create(CONF);
		
		Option property  = OptionBuilder.withArgName("property=value")
                .hasArgs(2)
                .withValueSeparator()
                .withDescription( "use value for given property" )
                .create( "D" );
		
		Option helpOption = OptionBuilder.withArgName(HELP).create(HELP);

		// Declare the options
		Options opts = new Options();
		opts.addOption(modeOption);
		opts.addOption(helpOption);
		opts.addOption(confOption);
		opts.addOption(property);

		return opts;
	}
	
	private static void failAndExit(String msg) {
		System.err.println();
		System.err.println(msg);
		System.err.println();
		printUsage(System.err);
		System.exit(-1);
	}
	
	private static CommandLine parseAndValidateInput(String[] args) {

		// Make sure we have some
		if (args == null || args.length == 0) {
			//printUsage(System.out);
			//System.exit(0);
		}

		// Parse the arguments
		Options opts = buildOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(opts, args, true);
		} catch (ParseException e) {
			failAndExit("Unable to parse the input arguments:\n"
					+ e.getMessage());
		}

		// Ensure we don't have any extra input arguments
		if (line.getArgs() != null && line.getArgs().length > 0) {
			System.err.println("Unsupported input arguments:");
			for (String arg : line.getArgs()) {
				System.err.println(arg);
			}
			failAndExit("");
		}

		// If the user asked for help, nothing else to do
		if (line.hasOption(HELP)) {
			return line;
		}

		if (!line.hasOption(MODE)) {
			failAndExit("The 'mode' option is required");
		}		
		
		String mode = line.getOptionValue(MODE);
		
		if (! (mode.equals(MODE_DATAGEN) || mode.equals(MODE_REFRESH))) {
			failAndExit("Only support modes:"+MODE_DATAGEN+","+MODE_REFRESH);
		}
		
		Properties properties = line.getOptionProperties("D");
		
		if (properties.getProperty(Constants.BIGFRAME_HADOOP_HOME) == null) {
			failAndExit("HADOOP_HOME is not set");
		}
		
		if (properties.getProperty(Constants.BIGFRAME_HADOOP_SLAVE) == null) {
			failAndExit("HADOOP_SLAVES is not set");
		}
		
		if (properties.getProperty(Constants.BIGFRAME_TPCDS_LOCAL) == null) {
			failAndExit("TPCDS_LOCAL is not set");
		}
		
		if (properties.getProperty(Constants.BIGFRAME_TPCDS_SCRIPT) == null) {
			failAndExit("TPCDS_SCRIPT is not set");
		}
		
		if (properties.getProperty(Constants.BIGFRAME_CONF_DIR) == null) {
			failAndExit("CONF_DIR is not set");
		}
		
		
		return line;
	}
	
	
	
	
	public static void main(String[] args) {
		// Get the input arguments
		CommandLine line = parseAndValidateInput(args);

		// Print out instructions details if asked for
		if (line.hasOption(HELP)) {
			printUsage(System.out);
			System.exit(0);
		}
		
		//System.out.println(new File(".").getAbsolutePath());
		InputStream default_conf_file = BigFrameDriver.class.getClassLoader().getResourceAsStream("default.xml");
		XMLBenchmarkSpecsParser parser = new XMLBenchmarkSpecsParser();
		BenchmarkConf conf = parser.importXML(default_conf_file);
		
		File user_conf_file = new File(line.getOptionProperties("D").getProperty(Constants.BIGFRAME_CONF_DIR)+"/"+"bigframe-core.xml");
		BenchmarkConf user_conf = parser.importXML(user_conf_file);
		

		// Replace conf with user define parameter
		Map<String,String> user_datagen_conf = user_conf.getDatagenConf().getProp();
		Map<String,String> user_querygen_conf = user_conf.getQuerygenConf().getProp();
		
		for (Map.Entry<String, String> entry : user_datagen_conf.entrySet()) {
			conf.getDatagenConf().set(entry.getKey(), entry.getValue());
		}
		
		for (Map.Entry<String, String> entry : user_querygen_conf.entrySet()) {
			conf.getQuerygenConf().set(entry.getKey(), entry.getValue());
		}
		
		// System global variables
		Properties properties = line.getOptionProperties("D");
		Set<Object> states = properties.keySet(); // get set-view of keys 
		for (Object object : states) {
			String key = (String) object;
			String value = properties.getProperty(key);
			conf.getDatagenConf().set(key, value);

		}
		
		conf.printConf();
		
		//CollectTPCDSstatNaive tpcds = new CollectTPCDSstatNaive();
		//PromotionInfo test = tpcds.getPromotInfo(conf.getDatagenConf(), 1);
		//tpcds.genPromtTBLonHDFS(conf.getDatagenConf(), 100);
/*		ArrayList<Integer> dateBeginSK = test.getDateBeginSK();
		ArrayList<Integer> dateEndSK = test.getDateEndSK();
		
		for(int i = 0; i < dateBeginSK.size(); i++) {
			System.out.println("Date begin: " + test.getDateBySK(dateBeginSK.get(i)) + ";" + "Date end: " + test.getDateBySK(dateEndSK.get(i)));
		}
*/
		
		
		//System.exit(-1);
		
		//If mode equals datagen, then generate the data we need
		if ( line.getOptionValue(MODE).equals(MODE_DATAGEN)) {
			DatagenFactory datagen_factory = new DatagenFactory(conf.getDatagenConf());
			
			List<DataGenerator> datagen_list = datagen_factory.createGenerators();
			
			for(DataGenerator datagen : datagen_list) {
				datagen.generate();
			}
		}
		
	}
}
