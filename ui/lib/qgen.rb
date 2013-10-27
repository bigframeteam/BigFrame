class Qgen

  def initialize(home)
    @bigframe_home=home
  end

  def perform
    $stdout.reopen('log/qgen.log','w')
    $stdout.sync=true
    system(@bigframe_home + "/bin/qgen -mode runqueries 2>&1")
    $stdout.sync=false
  end

end