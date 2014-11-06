package bigframe.util.dataloader;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.fs.Path;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat;

import bigframe.util.Constants;
//import bigframe.util.TableNotFoundException;
import bigframe.util.dataloader.vertica.VerticaDataLoader;
import bigframe.util.dataloader.vertica.VerticaGraphLoader;
import bigframe.util.dataloader.vertica.VerticaTpcdsLoader;
import bigframe.util.dataloader.vertica.VerticaTweetLoader;

public class DataLoaderDriver {


	/* ***************************************************************
	 * DATA MEMBERS
	 * ***************************************************************
	 */

	private static final Log LOG = LogFactory.getLog(DataLoaderDriver.class);

	private static Map<String, HashSet<String> > supported_engines = new HashMap<String, HashSet<String> >();

	static {
		HashSet<String> temp = new HashSet<String>();
		
		temp.add(Constants.HADOOP);
		
		supported_engines.put(Constants.VERTICA, temp);
	}
	
	

	// Main parsing options
	private static String FROM = "from";
	private static String TO = "to";
	private static String SRC = "src";
	private static String APPDOMAIN = "appdomain";
	private static String DATATYPE = "datatype";
	private static String CONF = "conf";
	private static String HELP = "help";



	private static void printUsage(PrintStream out) {

		out.println();
		out.println("Usage: dataload");
		out.println(" -from								[required] The engine that contain the source data");
		out.println(" -to								[required] The engine to store the data into");
		out.println(" -src							    [required] The source path");
		out.println(" -appdomain						[required] The Application Domain which the data belongs to");
		out.println(" -datatype							[required] The data type: e.g, relational, graph");
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
		Option fromOption = OptionBuilder.withArgName(FROM).hasArg()
				.withDescription("The engine that contain the source data").create(FROM);

		Option toOption = OptionBuilder.withArgName(TO).hasArg()
				.withDescription("The engine to store the data into").create(TO);
		
		Option srcOption = OptionBuilder.withArgName(SRC).hasArg()
				.withDescription("The source path").create(SRC);
		
//		Option desOption = OptionBuilder.withArgName(DES).hasArg()
//				.withDescription("The des path").create(DES);
		
		Option appdomainOption = OptionBuilder.withArgName(APPDOMAIN).hasArg()
				.withDescription("The Application Domain which the data belongs to").create(APPDOMAIN);
		
		Option datatypeOption = OptionBuilder.withArgName(DATATYPE).hasArg()
				.withDescription("The data type: e.g, relational, graph").create(DATATYPE);
		
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
		opts.addOption(fromOption);
		opts.addOption(toOption);
		opts.addOption(srcOption);
//		opts.addOption(desOption);
		opts.addOption(appdomainOption);
		opts.addOption(datatypeOption);
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

		if (!line.hasOption(FROM) || !line.hasOption(TO) || !line.hasOption(SRC) 
				|| !line.hasOption(APPDOMAIN) ||  !line.hasOption(DATATYPE)) {
			failAndExit("The 'from', 'to', 'src', 'des', 'appdomain' and 'datatype' options all are required");
		}

		String from = line.getOptionValue(FROM);
		String to = line.getOptionValue(TO);
		String src = line.getOptionValue(SRC);
//		String des = line.getOptionValue(DES);

		if(supported_engines.containsKey(from)) {
			if(!supported_engines.get(from).contains(to)) {
				System.out.println("Doesn't support loading data from " + from + " to " + to);
				
				System.out.println("The supported destination for " + from + " are:");
				
				for(String value : supported_engines.get(from)) {
					System.out.println(value);
				}
			}
		}
		
		else {
			System.out.println("Not supported data source: " + from);
			System.out.println("Only these are supported");
			
			for(String value : supported_engines.keySet()) {
				System.out.println(value);
			}
		}
		
		Properties properties = line.getOptionProperties("D");

		if(from.equals(Constants.HADOOP) || to.equals(Constants.HADOOP)) {
			if (properties.getProperty(BigConfConstants.BIGFRAME_HADOOP_HOME) == null) {
				failAndExit("HADOOP_HOME is not set");
			}
		}
		
		if(from.equals(Constants.VERTICA) || to.equals(Constants.VERTICA)) {
			if (properties.getProperty(BigConfConstants.BIGFRAME_VERTICA_DATABASE) == null || 
					properties.getProperty(BigConfConstants.BIGFRAME_VERTICA_HOSTNAMES) == null ||
					properties.getProperty(BigConfConstants.BIGFRAME_VERTICA_PORT) == null ||
					properties.getProperty(BigConfConstants.BIGFRAME_VERTICA_USERNAME) == null ||
					properties.getProperty(BigConfConstants.BIGFRAME_VERTICA_PASSWORD) == null) {
				failAndExit("These parameters should be set: \n" + 
						BigConfConstants.BIGFRAME_VERTICA_DATABASE + "\n" + 
						BigConfConstants.BIGFRAME_VERTICA_HOSTNAMES + "\n" + 
						BigConfConstants.BIGFRAME_VERTICA_PORT + "\n" + 
						BigConfConstants.BIGFRAME_VERTICA_USERNAME + "\n" + 
						BigConfConstants.BIGFRAME_VERTICA_PASSWORD + "\n" );
			}
		}


		return line;
	}

	/**
	 * Call the proper loader to finish the data loading.
	 * @param line
	 * @param workflowIF
	 */
	private static void load(CommandLine line, WorkflowInputFormat workflowIF) {
		
		if(line.getOptionValue(FROM).equals(Constants.HADOOP) && 
				line.getOptionValue(TO).equals(Constants.VERTICA)) {
			if(line.getOptionValue(APPDOMAIN).equals(BigConfConstants.APPLICATION_BI)) {
				if(line.getOptionValue(DATATYPE).equals(Constants.RELATIONAL)) {
					VerticaDataLoader dataloader = new VerticaTpcdsLoader(workflowIF);			
					
					try {
						dataloader.prepareBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					LOG.info("Begin loading the relational data");
					
					dataloader.load(new Path(line.getOptionValue(SRC)+"/store_sales"), "store_sales");
					dataloader.load(new Path(line.getOptionValue(SRC)+"/catalog_sales"), "catalog_sales");
					//dataloader.load(new Path(line.getOptionValue(SRC)+"/customer"), "customer");
					dataloader.load(new Path(line.getOptionValue(SRC)+"/web_sales"), "web_sales");
					dataloader.load(new Path(line.getOptionValue(SRC)+"/item"), "item");
					dataloader.load(new Path(line.getOptionValue(SRC)+"/promotion"), "promotion");
					dataloader.load(new Path(line.getOptionValue(SRC)+"/date_dim"), "date_dim");
					
					LOG.info("End loading the relational data");
					try {
						dataloader.alterBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				else if(line.getOptionValue(DATATYPE).equals(Constants.NESTED)) {
					VerticaDataLoader dataloader = new VerticaTweetLoader(workflowIF);

					//dataloader.load(new Path(line.getOptionValue(SRC)), "tweetjson");
					
					try {
						dataloader.prepareBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					LOG.info("Begin loading the tweet data");
					String TWEET_STORE_TYPE = workflowIF.get().containsKey(BigConfConstants.TWEET_STORE_FORMAT) 
							? workflowIF.get().get(BigConfConstants.TWEET_STORE_FORMAT) : "" ;
					
					if(TWEET_STORE_TYPE.equals(BigConfConstants.TWEET_AS_STRING))
						dataloader.load(new Path(line.getOptionValue(SRC)), "tweetjson");
					else if(TWEET_STORE_TYPE.equals(BigConfConstants.TWEET_NORMALIZED)) {
						dataloader.load(new Path(line.getOptionValue(SRC)), "users");
						dataloader.load(new Path(line.getOptionValue(SRC)), "tweet");
						dataloader.load(new Path(line.getOptionValue(SRC)), "entities");
					}
					else {
						LOG.error("Please Specify the storage format for tweet data. Available types:" 
								+ BigConfConstants.TWEET_AS_STRING + "," + BigConfConstants.TWEET_NORMALIZED);
						LOG.error("For example: " + "-D" + BigConfConstants.TWEET_STORE_FORMAT +"="+BigConfConstants.TWEET_AS_STRING);
						System.exit(-1);
					}
					LOG.info("End loading the tweet data");
					
					try {
						dataloader.alterBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
				else if(line.getOptionValue(DATATYPE).equals(Constants.GRAPH)) {
					VerticaDataLoader dataloader = new VerticaGraphLoader(workflowIF);
					
					try {
						dataloader.prepareBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					LOG.info("Begin loading the graph data");
					
					dataloader.load(new Path(line.getOptionValue(SRC)), "twitter_graph");
					
					LOG.info("End loading the graph data");
					
					try {
						dataloader.alterBaseTable();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				else {
					System.out.println("Not supported data type: " + line.getOptionValue(DATATYPE));
				}
				
			}
			else {
				System.out.println("Not supported data domain: " + line.getOptionValue(APPDOMAIN));
			}
		}
		
	}

	public static void main(String[] args) {
		// Get the input arguments
		CommandLine line = parseAndValidateInput(args);

		// Print out instructions details if asked for
		if (line.hasOption(HELP)) {
			printUsage(System.out);
			System.exit(0);
		}

		WorkflowInputFormat workflowIF = new WorkflowInputFormat();
		
		// System global variables
		Properties properties = line.getOptionProperties("D");
		Set<Object> states = properties.keySet(); // get set-view of keys
		for (Object object : states) {
			String key = (String) object;
			String value = properties.getProperty(key);
			workflowIF.set(key, value);

		}

		workflowIF.printConf();
		
		// Load the data set from src to des
		load(line, workflowIF);


	}


}
