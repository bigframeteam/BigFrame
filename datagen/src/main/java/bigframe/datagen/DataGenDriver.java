package bigframe.datagen;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import bigframe.bigif.BigFrameInputFormat;
import bigframe.bigif.BigConfConstants;
import bigframe.refreshing.RefreshDriver;
import bigframe.refreshing.driver.KafkaRefreshDriver;
import bigframe.util.parser.XMLBigFrameInputParser;



/**
 * The entrance of BigFrame data generator.
 * 
 * @author andy
 * 
 */

public class DataGenDriver {



	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	//private static final Log LOG = LogFactory.getLog(DataGenDriver.class);



	// Main parsing options
	private static String MODE = "mode";
	private static String CONF = "conf";
	private static String HELP = "help";
	private static String MODE_DATAGEN = "datagen";
	private static String MODE_REFRESH = "refresh";





	private static void printUsage(PrintStream out) {

		out.println();
		out.println("Usage: datagen");
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

		if (properties.getProperty(BigConfConstants.BIGFRAME_HADOOP_HOME) == null) {
			failAndExit("HADOOP_HOME is not set");
		}

		if (properties.getProperty(BigConfConstants.BIGFRAME_HADOOP_SLAVE) == null) {
			failAndExit("HADOOP_SLAVES is not set");
		}

		if (properties.getProperty(BigConfConstants.BIGFRAME_TPCDS_LOCAL) == null) {
			failAndExit("TPCDS_LOCAL is not set");
		}

		if (properties.getProperty(BigConfConstants.BIGFRAME_TPCDS_SCRIPT) == null) {
			failAndExit("TPCDS_SCRIPT is not set");
		}

		if (properties.getProperty(BigConfConstants.BIGFRAME_CONF_DIR) == null) {
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
		InputStream default_conf_file = BigFrameInputFormat.class.getClassLoader().getResourceAsStream("default.xml");
		XMLBigFrameInputParser parser = new XMLBigFrameInputParser();
		BigFrameInputFormat conf = parser.importXML(default_conf_file);

		File user_conf_file = new File(line.getOptionProperties("D").getProperty(BigConfConstants.BIGFRAME_CONF_DIR)+"/"+"bigframe-core.xml");
		BigFrameInputFormat user_conf = parser.importXML(user_conf_file);


		// Replace conf with user define parameter
		Map<String,String> user_datagen_conf = user_conf.getBigDataInputFormat().getProp();
		Map<String,String> user_querygen_conf = user_conf.getBigQueryInputFormat().getProp();

		for (Map.Entry<String, String> entry : user_datagen_conf.entrySet()) {
			conf.getBigDataInputFormat().set(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, String> entry : user_querygen_conf.entrySet()) {
			conf.getBigQueryInputFormat().set(entry.getKey(), entry.getValue());
		}

		// System global variables
		Properties properties = line.getOptionProperties("D");
		Set<Object> states = properties.keySet(); // get set-view of keys
		for (Object object : states) {
			String key = (String) object;
			String value = properties.getProperty(key);
			conf.getBigDataInputFormat().set(key, value);

		}

		conf.printConf();


		//If mode equals datagen, then generate the data we need
		if ( line.getOptionValue(MODE).equals(MODE_DATAGEN)) {
//			DatagenFactory datagen_factory = new DatagenFactory(conf.getBigDataInputFormat());

			List<DataGenerator> datagen_list = DatagenFactory.createGenerators(conf.getBigDataInputFormat());

			for(DataGenerator datagen : datagen_list) {
				datagen.generate();
			}
		}
		
		else if (line.getOptionValue(MODE).equals(MODE_REFRESH)) {
			RefreshDriver refresh_driver = new KafkaRefreshDriver(conf.getBigDataInputFormat());
			
			System.out.println("Preparing the refreshing data...");
			refresh_driver.prepare();			
			System.out.println("Finish data preparation!");
			
			refresh_driver.refresh();
		}

	}
}
