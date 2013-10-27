class ExecutionController < ApplicationController
  before_filter :authenticate, :only=>["index", "new", "submit"]
  require 'parser'

  def index
  end
end
