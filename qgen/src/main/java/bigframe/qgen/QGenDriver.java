package bigframe.qgen;


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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigframe.bigif.BigFrameInputFormat;
import bigframe.bigif.BigConfConstants;
import bigframe.qgen.engineDriver.EngineDriver;
import bigframe.qgen.factory.WorkflowFactory;
import bigframe.util.parser.XMLBigFrameInputParser;

/**
 * Entrance of the workflow running program
 * @author andy
 *
 */
public class QGenDriver {



	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(QGenDriver.class);



	// Main parsing options
	private static String MODE = "mode";
	private static String CONF = "conf";
	private static String HELP = "help";
	private static String MODE_RUN_QUERY = "runqueries";
	private static String MODE_GEN_QUERY = "genqueries";
	private static String ENABLE_PROFILE = "enableprofile";



	private static void printUsage(PrintStream out) {

		out.println();
		out.println("Usage: qgen");
		out.println(" -mode Currently, these modes are supportted:");
		out.println("\t[genqueries, runqueries]");
		out.println(" -conf <config file> Benchmark configuration file");
		out.println(" -help	Print this usage");
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

		//Option enableProfileOption = OptionBuilder.withArgName(ENABLE_PROFILE).hasArg(false)
		//		.withArgName("Enable profile when benchmarking").create(ENABLE_PROFILE);
		
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
		//opts.addOption(enableProfileOption);
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
			printUsage(System.out);
			System.exit(0);
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

		if (!mode.equals(MODE_RUN_QUERY)) {
			failAndExit("Only support modes:"+MODE_RUN_QUERY);
		}

		Properties properties = line.getOptionProperties("D");

//		if (properties.getProperty(BigConfConstants.BIGFRAME_HADOOP_HOME) == null) {
//			failAndExit("HADOOP_HOME is not set");
//		}
//
//		if (properties.getProperty(BigConfConstants.BIGFRAME_HADOOP_SLAVE) == null) {
//			failAndExit("HADOOP_SLAVES is not set");
//		}
//
//		if (properties.getProperty(BigConfConstants.BIGFRAME_TPCDS_LOCAL) == null) {
//			failAndExit("TPCDS_LOCAL is not set");
//		}
//
//		if (properties.getProperty(BigConfConstants.BIGFRAME_TPCDS_SCRIPT) == null) {
//			failAndExit("TPCDS_SCRIPT is not set");
//		}

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


		// Replace conf which user define in the bigframe-core config file
		Map<String,String> user_bigdata_inputformat = user_conf.getBigDataInputFormat().get();
		Map<String,String> user_bigquery_inputformat = user_conf.getBigQueryInputFormat().get();

		for (Map.Entry<String, String> entry : user_bigdata_inputformat.entrySet()) {
			conf.getBigDataInputFormat().set(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, String> entry : user_bigquery_inputformat.entrySet()) {
			conf.getBigQueryInputFormat().set(entry.getKey(), entry.getValue());
		}

		// All configuration will be overrode by those specified in cmd 
		Properties properties = line.getOptionProperties("D");
		Set<Object> states = properties.keySet(); // get set-view of keys
		for (Object object : states) {
			String key = (String) object;
			String value = properties.getProperty(key);
			conf.getBigDataInputFormat().set(key, value);
			conf.getBigQueryInputFormat().set(key, value);
			conf.getWorkflowInputFormat().set(key, value);
		}

		conf.printConf();


		//If mode equals run-query, then collect the set of hard-coded queries and 
		// delegate the job to their corresponding driver to run them
		
		if ( line.getOptionValue(MODE).equals(MODE_RUN_QUERY)) {
			WorkflowFactory workflowFactory = new WorkflowFactory(conf);
			
			List<EngineDriver> workflows = workflowFactory.createWorkflows();
			if(workflows != null)
				for(EngineDriver workflow : workflows) {
					workflow.init();
					workflow.run();
					workflow.cleanup();
				}
		}
		
		// TODO: Only output the specifications of the set of queries to be run.
		
		else if ( line.getOptionValue(MODE).equals(MODE_GEN_QUERY)) {

		}

	}
}
