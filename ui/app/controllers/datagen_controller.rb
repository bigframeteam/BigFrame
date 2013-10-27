class DatagenController < ApplicationController
  require 'datagen'
  require 'parser'

  #apply authentication 
  before_filter :authenticate, :only=>["index", "new", "submit"]

  #list of the parameters
  @xml
  @log
  @name
  @bigframe_home
  @tpchds_local
  @data_volume 
  @data_variety
  @data_variety_rel
  @data_variety_text
  @data_variety_graph
  @data_velocity 
  @data_output

  def index
  end

  #This is called when we click the generate data.
  def submit
    @name = params[:name]
    @bigframe_home = params[:bigframe_home]
    @hadoop_home = params[:hadoop_home]
    @tpchds_local = params[:tpchds_local]
    @data_volume = params[:data_volume]
    @data_variety_rel = params[:relational]
    @data_variety_text = params[:text]
    @data_variety_graph = params[:graph]
    @datavariety_graph = ""
    @data_velocity = params[:data_velocity]
    @data_output = params[:hdfs_path]
    process_config
    process_xml
    Delayed::Job.enqueue(Datagen.new(@bigframe_home))
    redirect_to :action=>"index"
  end

  #read the log file and put the text into @log
  def log
    file = File.open('log/datagen.log')
    @log = file.read
  end

  #fill in the config file with the user's input
  def process_config
    input = File.open("lib/templates/config_template.sh","r")
    config = input.read
    input.close()
    output = File.new(@bigframe_home + "/conf/config.sh", "w")      
    if output
      output.syswrite(config %[@hadoop_home, @tpchds_local])
    end
    output.close()
  end

  # modify xml file based on the user input from the ui.
  def process_xml
    parser = Parser.new(@bigframe_home)
    @xml = parser.parse_xml
    volume = "bigframe.datavolume"
    path_rel = "bigframe.data.hdfspath.relational"
    path_graph = "bigframe.data.hdfspath.graph"
    path_nested = "bigframe.data.hdfspath.nested"
    path_text = "bigframe.data.hdfspath.text"
    data_variety = "bigframe.datavariety"
    @data_variety = ""
    if @data_variety_rel == "1"
      @data_variety = @data_variety + "relational, "
    end
    if @data_variety_text == "1"
      @data_variety = @data_variety + "nested, "
    end
    if @data_variety_graph == "1"
      @data_variety = @data_variety + "graph"
    end
    if @data_variety[-2] == ","
      @data_variety = @data_variety[0..-3]
    end
    @xml[volume] = @data_volume
    @xml[path_rel] = @data_output + "/" + @xml[path_rel]
    @xml[path_graph] = @data_output + "/" + @xml[path_graph] 
    @xml[path_nested] = @data_output + "/" + @xml[path_nested]
    @xml[path_text] = @data_output + "/" + @xml[path_text]
    @xml[data_variety] = @data_variety
    parser.build_xml(@xml)
  end
end
