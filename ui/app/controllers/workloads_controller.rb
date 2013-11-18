class WorkloadsController < ApplicationController

LOG_PATH=ENV['HOME']+'/tmp/'

def index
	@job_info={}
	#loop through each file in directory, each is a job
	Dir.foreach(LOG_PATH) do |item|
		next if item.start_with?('.')
		hash={}
		hash["name"]=item
		hash["age"]=seconds_to_units(Time.now-File.ctime(LOG_PATH+item))
		@job_info[item]=hash
	end
end

  def visualize
  	@jsondata=JSON.parse(File.read(LOG_PATH+params[:json]))
  end
end
