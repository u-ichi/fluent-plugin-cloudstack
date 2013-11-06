module Fluent
  class CloudStackInput < Input
    Fluent::Plugin.register_input("cloudstack", self)

    INTERVAL_MIN = 300

    config_param :host
    config_param :apikey
    config_param :secretkey

    config_param :path,        :default => '/client/api'
    config_param :protocol,    :default => 'https'
    config_param :port,        :default => 443
    config_param :domain_id,   :default => nil
    config_param :tag,         :default => "cloudstack"
    config_param :ssl,         :default => true
    config_param :debug_mode,  :default => false
    config_param :interval,    :default => INTERVAL_MIN

    attr_writer :before_events

    def initialize
      require "fog"
      require "eventmachine"
      init_eventmachine

      super
    end

    def configure(conf)
      super

      @conf = conf

      unless @host && @apikey && @secretkey
        raise ConfigError, "'host' and 'apikey' and 'secretkey' must be all specified."
      end

      unless @debug_mode
        if @interval.to_i < INTERVAL_MIN
          raise ConfigError, "'interval' must be over #{INTERVAL_MIN}."
        end
      end

      @before_events_filepath = "logs/before_events.#{tag}.yml"

      if File.exist?(@before_events_filepath)
        @before_events = YAML.load_file(@before_events_filepath)
      else
        @before_events = nil
      end

      @before_usages_filepath = "logs/before_usages.#{tag}.yml"
      if File.exist?(@before_usages_filepath)
        @before_usages = YAML.load_file(@before_usages_filepath)
      else
        @before_usages = Hash.new
      end

      @event_output_tag = "#{@tag}.event"
      @usages_output_tag = "#{@tag}.usages"

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
        begin
          emit_new_events
          emit_usages
        rescue => ex
          $log.warn("EM.periodic_timer loop error.")
          $log.warn("#{ex}, tracelog : \n#{ex.backtrace.join("\n")}")
        end
      end
    end

    def emit_new_events
      new_events = get_new_events
      new_events.each do |event|
        time = Time.parse(event["created"]).to_i
        Engine.emit(@event_output_tag, time, event)
      end

      Engine.emit("#{@usages_output_tag}", Engine.now, {"events_flow" => new_events.size})
    end

    def emit_usages
      usages = get_usages

      Engine.emit("#{@usages_output_tag}", Engine.now, get_usages)
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
      memory_sum      = 0
      cpu_sum         = 0
      root_volume_sum = 0
      data_volume_sum = 0

      vms_responses = cs.list_virtual_machines(:domainid=>@domain_id)
      vms =  vms_responses["listvirtualmachinesresponse"]["virtualmachine"]

      if vms
        vms.each do |vm|
          memory_sum += vm["memory"].to_i * 1024 * 1024
          cpu_sum += vm["cpunumber"].to_i
          usages_per_service_offering[vm["serviceofferingname"]] += 1
        end
      else
        vms = []
      end

      volumes_responses = cs.list_volumes(:domainid=>@domain_id)
      volumes =  volumes_responses["listvolumesresponse"]["volume"]

      if volumes
        volumes.each do |volume|
          case volume["type"]
          when "ROOT"
            root_volume_sum += volume["size"]
          when "DATADISK"
            data_volume_sum += volume["size"]
            usages_per_disk_offering[volume["diskofferingname"].gsub(' ','_')] += 1
          end
        end
      end

      usages = Hash.new

      usages[:vm_sum]          = vms.size
      usages[:memory_sum]      = memory_sum
      usages[:cpu_sum]         = cpu_sum
      usages[:root_volume_sum] = root_volume_sum
      usages[:data_volume_sum] = data_volume_sum

      usages_per_service_offering.each do |key,value|
        usages[key] = value
      end
      usages_per_disk_offering.each do |key,value|
        usages[key] = value
      end

      @before_usages.each do |key,value|
        unless usages.key?(key)
          usages[key] = 0
        end
      end

      File.write(@before_usages_filepath, usages.to_yaml)
      @before_usages = usages

      usages
    end

    def cs
      @cs ||= Fog::Compute.new(
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
        $log.fatal("Eventmachine problem")
        $log.fatal("#{ex}, tracelog : \n#{ex.backtrace.join("\n")}")
      end
    end
  end
end

