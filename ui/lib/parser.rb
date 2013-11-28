class Parser
  require "rexml/document"
  require 'builder'
  include REXML

  def initialize(home)
    @bigframe_home=home
  end

  #parsing the original xml file and keep the information in @xml.
  def parse_xml
    xml_hash = Hash.new

    file = File.open("lib/templates/bigframe-core_template.xml","r") 

    doc2 = Document.new file
    doc2.elements.each("configuration/property") do |element| 
      if element != nil then
        name = element.elements["name"].text unless element.elements["name"].nil?
        value = element.elements["value"].text unless element.elements["value"].nil?
        xml_hash[name] = value
      end
    end
    return xml_hash
  end

  # rebuild the xml modified xml file
  def build_xml(hash)
    result_xml = ""
    builder = Builder::XmlMarkup.new(:target=>result_xml,:indent=>4)
    builder.tag!("configuration") { 
    hash.each do|name,value|
        builder.property { |b| b.name(name); b.value(value) }
      end
    }
    file = File.new(@bigframe_home + "/conf/bigframe-core.xml", "w")
    if file
      file.syswrite(result_xml)
    end
    file.close()
  end


end