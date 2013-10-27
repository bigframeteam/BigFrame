class ApplicationController < ActionController::Base
  # Prevent CSRF attacks by raising an exception.
  # For APIs, you may want to use :null_session instead.
  protect_from_forgery with: :exception

  def current_user  
    @current_user ||= User.find(session[:user_id]) if session[:user_id]  
    rescue ActiveRecord::RecordNotFound
  end  

  def authenticate
	  if current_user.nil?
	  redirect_to :controller=>"sessions", :action=>"new"
	  end
  end

  helper_method :current_user, :authenticate
end
