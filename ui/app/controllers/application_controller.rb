class ApplicationController < ActionController::Base
  # Prevent CSRF attacks by raising an exception.
  # For APIs, you may want to use :null_session instead.
  protect_from_forgery with: :exception

   def current_user  
    @current_user ||= User.find(session[:user_id]) if session[:user_id]  
  end  

def authenticate
	if current_user.nil?
	redirect_to :controller=>"sessions", :action=>"new"
	end
end

def seconds_to_units(seconds)
  '%dd %dh %dm %ds' %
    # the .reverse lets us put the larger units first for readability
    [24,60,60].reverse.inject([seconds]) {|result, unitsize|
      result[0,0] = result.shift.divmod(unitsize)
      result
    }
end

  helper_method :current_user, :authenticate, :seconds_to_units
end
