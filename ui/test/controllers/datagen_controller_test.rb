require 'test_helper'

class DatagenControllerTest < ActionController::TestCase
  test "should get index" do
    get :index
    assert_response :success
  end

end
