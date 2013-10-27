module Fluent
  class CloudStackInput < Input
    Fluent::Plugin.register_input("cloudstack", self)

    INTERVAL_MIN = 300

    config_param :host
    config_param :path, :default =>'/client/api'
    config_param :protocol, :default =>'https'
    config_param :port, :default =>443
    config_param :apikey
    config_param :secretkey
    config_param :domain_id, :default => nil
    config_param :interval, :default => INTERVAL_MIN

    config_param :tag, :string, :default => "cloudstack"
    config_param :ssl, :bool, :default => true

    attr_writer :before_events

    def initialize
      require "fog"
      require "eventmachine"
      init_eventmachine

      @before_events_filepath = "logs/before_events.yml"

      if File.exist?(@before_events_filepath)
        @before_events = YAML.load_file(@before_events_filepath)
      else
        @before_events = nil
      end

      super
    end

    def configure(conf)
      super
      @conf = conf
      unless @host && @apikey && @secretkey
        raise ConfigError, "'host' and 'apikey' and 'secretkey' must be all specified."
      end

      if @interval < INTERVAL_MIN
        raise ConfigError, "'interval' must be over #{INTERVAL_MIN}."
      end
    end

    def start
      super
      run_reactor_thread
      @thread = Thread.new(&method(:run))
      $log.info "listening cloudstack api on #{@host}"
    end

    def shutdown
      super
      @thread.join
      EM.stop if EM.reactor_running?
      @reactor_thread.join if @reactor_thread
    end

    def run
      EM.add_periodic_timer(@interval) do
        emit_new_events
        emit_usages
      end
    end

    def emit_new_events
      new_events = get_new_events
      output_tag = "#{@tag}.event"
      new_events.each do |event|
        time = Time.parse(event["created"]).to_i
        Engine.emit(output_tag, time, event)
      end
    end

    def emit_usages
      usages = get_usages
      time = Engine.now
      output_tag = "#{@tag}.usages"
      Engine.emit(output_tag, time, usages)
    end

    def get_new_events
      if @before_events
        startdate = Time.parse(@before_events[0]["created"])
        event_responses = cs.list_events(:startdate => startdate.strftime("%Y-%m-%d %H:%M:%S"), :domainid => @domain_id)
        events = Array.new
        event_responses["listeventsresponse"]["event"].each do |event|
          unless Time.parse(event["created"]) == startdate
            events.push event
          end
        end
      else
        events = cs.list_events(:domainid => @domain_id)["listeventsresponse"]["event"]
      end

      if events.size > 0
        File.write(@before_events_filepath, events.to_yaml)
        @before_events = events
      end

      events
    end

    def get_usages
      usages_per_service_offering   = Hash.new(0)
      usages_per_disk_offering      = Hash.new(0)
      memory_usage      = 0
      cpu_usage         = 0
      root_volume_usage = 0
      data_volume_usage = 0

      vms_responses = cs.list_virtual_machines(:domainid=>@domain_id)
      vms =  vms_responses["listvirtualmachinesresponse"]["virtualmachine"]
      vms.each do |vm|
        memory_usage += vm["memory"].to_i
        cpu_usage += vm["cpunumber"].to_i
        usages_per_service_offering[vm["serviceofferingname"]] += 1
      end

      volumes_responses = cs.list_volumes(:domainid=>@domain_id)
      volumes =  volumes_responses["listvolumesresponse"]["volume"]
      volumes.each do |volume|
        case volume["type"]
        when "ROOT"
          root_volume_usage += volume["size"]
        when "DATADISK"
          data_volume_usage += volume["size"]
          usages_per_disk_offering[volume["diskofferingname"]] += 1
        end
      end

      results =  {:vm_usage             => vms.size,
                  :memory_usage                => memory_usage,
                  :cpu_usage                   => cpu_usage,
                  :root_volume_usage           => root_volume_usage,
                  :data_volume_usage           => data_volume_usage,
                  :usages_per_service_offering => usages_per_service_offering,
                  :usages_per_disk_offering    => usages_per_disk_offering,
      }
    end

    def cs
      @@cs ||= Fog::Compute.new(
          :provider => 'CloudStack',
          :cloudstack_api_key           => @apikey,
          :cloudstack_secret_access_key => @secretkey,
          :cloudstack_host              => @host,
          :cloudstack_port              => @port,
          :cloudstack_path              => @path,
          :cloudstack_scheme            => @protocol,
      )
    end

    private

    def run_reactor_thread
      unless EM.reactor_running?
        @reactor_thread = Thread.new { EM.run }
      end
    end

    def init_eventmachine
      EM.epoll; EM.kqueue
      EM.error_handler do |ex|
        $log.error("Eventmachine problem")
        $log.error("#{ex}, tracelog : \n#{ex.backtrace.join("\n")}")
      end
    end
  end
end

