class SessionsController < ApplicationController  
  def new  
  end  
  
def create  
  user = User.find_by_name(params[:name])  
  if user && user.authenticate(params[:password])  
    session[:user_id] = user.id  
    redirect_to root_url
  else  
    flash[:notice]="Invalid name or password"
    redirect_to :action=>"new"
  end  
end  
   
  def destroy  
    session[:user_id] = nil  
    redirect_to root_url 
  end  
end  