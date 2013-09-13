class UsersController < ApplicationController  
  def new
    @user = User.new  
  end  
  
  def create  
    @user = User.new(app_params)  
    if @user.save  
      redirect_to root_url, :notice => "Signed up!"  
    else  
      render "new"  
    end  
  end 

    def app_params
    params.require(:user).permit(:name, :password, :password_confirmation)
  end
end  