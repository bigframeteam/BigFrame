class ExecutionController < ApplicationController
  require 'qgen'
  require 'parser'

  #apply authentication 
  before_filter :authenticate, :only=>["index", "new", "submit"]

  #list of the parameters
  @xml
  @query_volume
  @query_variety
  @query_velocity
  @queryengine_relational
  @queryengine_text
  @queryengine_graph
  @hdfs_root_dir
  @bigframe_home 

  def index
  end

  #This is called when we click the generate data.
  def submit
  	@bigframe_home = "/home/shlee0605/big/big-0.2"
  	@query_volume = params[:query_volume]
  	@query_variety = params[:query_variety]
  	@query_variety2 = ""
  	@query_velocity = params[:query_velocity]
  	@queryengine_relational = params[:queryengine_relational]
  	@queryengine_text = params[:queryengine_nested]
  	@queryengine_graph = params[:queryengine_graph]
  	@hdfs_root_dir = params[:hdfs_root_dir]
  	@query_variety2 = ""

    if @query_variety.split(",").size > 1
      @query_variety2 = "macro"
    else
      @query_variety2 = "micro"
    end

    process_env
    process_xml
    Delayed::Job.enqueue(Qgen.new(@bigframe_home))
    redirect_to :action=>"log"
  end

  #read the log file and put the text into @log
  def log
    file = File.open('log/qgen.log')
    @log = file.read
  end

  #fill in the config file with the user's input
  def process_env
    input = File.open("lib/templates/hadoop-env_template.sh","r")
    config = input.read
    input.close()
    output = File.new(@bigframe_home + "/conf/hadoop-env.sh", "w")

    if output
      output.syswrite(config % @hdfs_root_dir)
    end
    output.close()
  end

  # modify xml file based on the user input from the ui.
  def process_xml
  	parser = Parser.new(@bigframe_home)
    @xml = parser.parse_xml

    data_variety = "bigframe.datavariety"
  	query_var = "bigframe.queryvariety"
  	engine_rel = "bigframe.queryengine.relational"
  	engine_text = "bigframe.queryengine.nested"
  	engine_graph = "bigframe.queryengine.graph"

    @xml[data_variety] = @query_variety
    @xml[query_var] = @query_variety2
    @xml[engine_rel] = @queryengine_relational
    @xml[engine_text] = @queryengine_text
    @xml[engine_graph] = @queryengine_graph
    parser.build_xml(@xml)

  end
end
