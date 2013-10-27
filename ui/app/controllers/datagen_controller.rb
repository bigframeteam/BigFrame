class DatagenController < ApplicationController
  require 'datagen'
  require "rexml/document"
  require 'builder'
  require 'parser'
  include REXML

  #apply authentication 
  before_filter :authenticate, :only=>["index", "new", "submit"]

  #list of the parameters
  @xml
  @xml_build
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
    puts @datavariety_grph
    @data_velocity = params[:data_velocity]
    @data_output = params[:hdfs_path]
    process_config
    parse_xml
    modify_xml
    build_xml
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
    input = File.open(@bigframe_home + "/conf/config_template.sh","r")
    config = input.read
    input.close()
    output = File.new(@bigframe_home + "/conf/config.sh", "w")      
    if output
      output.syswrite(config %[@hadoop_home, @tpchds_local])
    end
    output.close()
  end

  #parsing the original xml file and keep the information in @xml.
  def parse_xml
    @xml = Hash.new

    file = File.open(@bigframe_home+ "/conf/bigframe-core_template.xml","r") 

    doc2 = Document.new file
    doc2.elements.each("configuration/property") do |element| 
      if element != nil then
        name = element.elements["name"].text unless element.elements["name"].nil?
        value = element.elements["value"].text unless element.elements["value"].nil?
        @xml[name] = value
      end
    end
  end

  # modify 
  def modify_xml
    volume = "bigframe.datavolume"
    path_rel = "bigframe.data.hdfspath.relational"
    path_graph = "bigframe.data.hdfspath.graph"
    path_nested = "bigframe.data.hdfspath.nested"
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
    @xml[data_variety] = @data_variety
  end

  # rebuild the xml modified xml file
  def build_xml
    @xml_build = ""
    builder = Builder::XmlMarkup.new(:target=>@xml_build,:indent=>4)
    builder.tag!("configuration") { 
    @xml.each do|name,value|
        builder.property { |b| b.name(name); b.value(value) }
      end
    }
    file = File.new(@bigframe_home + "/conf/bigframe-core.xml", "w")
    if file
      file.syswrite(@xml_build)
    end
    file.close()
  end
end
