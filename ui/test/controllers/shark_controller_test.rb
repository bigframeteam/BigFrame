require 'test_helper'

class SharkControllerTest < ActionController::TestCase
  test "should get visualize" do
    get :visualize
    assert_response :success
  end

  test "should get workloads" do
    get :workloads
    assert_response :success
  end

end
