class Datagen

  require 'open3'

  def initialize(home)
    @bigframe_home=home
  end

  def perform
    
    $stdout.reopen('log/datagen.log','w')
    $stdout.sync=true
    system(@bigframe_home + "/bin/datagen -mode datagen 2>&1")
    $stdout.sync=false
  end

end