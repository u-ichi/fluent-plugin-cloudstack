require 'test/unit'
require 'test_helper'
require 'lib/fluent/plugin/in_cloudstack.rb'
require 'pp'


class CloudStackInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    host localhost
    apikey hoge
    secretkey fuga
    domain_id domain_id
  ]


  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::CloudStackInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 'localhost',      d.instance.host
    assert_equal 'hoge',           d.instance.apikey
    assert_equal 'fuga',           d.instance.secretkey
    assert_equal 'domain_id',      d.instance.domain_id
  end


  def test_get_events
    d = create_driver

    # d.instance.before_events = before_events_stub
  end

  def test_get_usage
    d = create_driver

    # d.instance.get_usages
  end

end



