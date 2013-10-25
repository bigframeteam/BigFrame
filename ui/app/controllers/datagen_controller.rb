class DatagenController < ApplicationController

  before_filter :authenticate, :only=>["index", "new", "submit"]

  def index
  end
end
