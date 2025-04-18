require 'aws-sdk-s3'
require 'avro'
require 'active_support/time'

module HeapRS3Segment
  class Loader
    # HEAP Data Schema
    # https://help.heap.io/hc/en-us/articles/18700033317020-Heap-Connect-Data-Schema

    AWS_S3_DEFAULT_REGION = 'us-east-1'

    attr_accessor :processor, :project_identifier, :aws_s3_bucket, :prompt, :process_single_sync,
      :identify_only_users, :alias_on_identify, :revenue_mapping, :revenue_fallback, :user_id_prop,
      :skip_types, :skip_tables, :skip_before, :skip_file, :s3, :alias_cache, :alias_cache_reverse,
      :session_cache

    def initialize(processor, project_identifier, aws_s3_bucket, aws_access_key_id, aws_secret_access_key, aws_region=nil)
      Time.zone = 'UTC'
      @alias_cache = {}
      @alias_cache_reverse = {}
      @session_cache = {}

      @processor = processor
      @project_identifier = project_identifier

      @s3 = Aws::S3::Client.new(
        access_key_id: aws_access_key_id,
        secret_access_key: aws_secret_access_key,
        region: aws_region || AWS_S3_DEFAULT_REGION
      )
      @aws_s3_bucket = aws_s3_bucket
      @aws_s3_bucket_prefix = MANIFEST_BUCKET_PREFIX

      @prompt = true
      @process_single_sync = true # stops after one sync is processed
      @skip_types = [] # [:page, :track, :identify, :alias]
      @skip_tables = ['_event_metadata']
      @skip_before = nil
      @skip_file = nil # skip file if it matches
      @identify_only_users = false # this is useful when doing initial import and we don't need to identify anonymous users
      @alias_on_identify = true # find migrated user in @alias_cache and fire alias events for identified users
      @revenue_mapping = {}
      @revenue_fallback = []
      @user_id_prop = 'identity'
    end

    def call
      scan_manifests.each do |obj|
        already_synced = begin
          @s3.head_object({ bucket: @aws_s3_bucket, key: "imported_#{obj.key}" }) && true
        rescue Aws::S3::Errors::NotFound
          nil
        end

        next if already_synced

        if @prompt
          require 'pry'
          logger.debug 'Ready to process ' + obj.key + ', type "exit!" to interrupt, "already_synced = true" to skip this sync, set @skip_types to skip certian event types and CTRL-D to continue'
          binding.pry

          next if already_synced
        end

        process_sync(obj)
        break if @process_single_sync
      end
    end

    def logger
      HeapRS3Segment.logger
    end

    def scan_manifests
      list_opts = { bucket: @aws_s3_bucket, prefix: @aws_s3_bucket_prefix, delimiter: '/' }
      all_objects = []
      while true
        resp = @s3.list_objects_v2(list_opts)
        all_objects += resp.contents

        break unless resp.next_continuation_token.present?

        list_opts[:continuation_token] = resp.next_continuation_token
      end
      all_objects.select { |obj| obj.key.match(MANIFEST_REGEXP) }.sort_by(&:key)
    end

    def mark_manifest_as_synced(obj)
      @s3.copy_object(
        copy_source: "#{@aws_s3_bucket}/#{obj.key}",
        bucket: @aws_s3_bucket,
        key: "imported_#{obj.key}"
      )
    end

    def process_sync(obj)
      # reset cachees on every sync
      @alias_cache.clear
      @alias_cache_reverse.clear
      @session_cache.clear

      start_time = Time.now.utc
      manifest = get_manifest(obj)
      process_manifest(manifest)
      mark_manifest_as_synced(obj)

      diff = Time.now.utc - start_time
      logger.info "Done syncing #{obj.key} in #{diff.to_i} seconds"
    end

    def get_manifest(obj)
      logger.info "Reading #{obj.key}"

      manifest = s3_get_file(obj)
      JSON.parse(manifest.body.read)
    end

    def process_manifest(manifest)
      logger.info "Processing manifest(dump_id: #{manifest['dump_id']})"

      # skip tables we don't need, e.g. "sessions"
      tables = manifest['tables'].reject { |table| @skip_tables.include?(table['name']) }

      # custom sorter - aliases, any events then pageviews, finally identify
      index_type_name = ->(table) {
        idx_type = case table['name']
        when 'user_migrations' then [1, :alias]
        when 'sessions' then [3, :session]
        when 'pageviews' then [4, :page]
        when 'users' then [5, :identify]
        else [2, :track]
        end
        idx_type << table['name']
      }
      tables.sort_by!(&index_type_name)

      tables.each do |table|
        table['type'] = index_type_name.call(table)[1]
        logger.info "Order key #{index_type_name.call(table)}"
      end

      tables.each do |table|
        process_table(table)
      end
    end

    def process_table(table)
      event_name = table['name'].split('_').map(&:capitalize).join(' ')
      logger.info "Processing table(#{table['name']}) - \"#{event_name}\" event"

      files = table['files'].sort

      if ['users', 'user_migrations'].include?(table['name'])
        files.sort_by! do |path|
          filename = path.split('/').last
          filename.split('_').first.to_i
        end
      end

      files.each do |file|
        next if @skip_types.include?(table['type'])
        process_file(file, table['type'], event_name)
      end
    end

    def process_file(file, type, event_name)
      # example skip logic
      # heap_rs3.skip_file = ->(file) do
      #   if file.match(/pageviews\/part-(\d+)/)
      #     # skip files with part number outside of 10000..10999
      #     # e.g. only process files that match pageviews/part-10???
      #     unless (10000..10999).include?(match[1].to_i)
      #       return true
      #     end
      #   end
      # end

      if @skip_file.is_a?(Proc) && skip_file.call(file)
        logger.info "Skipping file(#{file})"
        return
      end

      logger.info "Processing file(#{file})"

      load_start_time = Time.now.utc
      s3_file = s3_get_file(file)
      reader = Avro::IO::DatumReader.new
      avro = Avro::DataFile::Reader.new(s3_file.body, reader)
      load_diff = Time.now.utc - load_start_time

      counter = 0
      skipped = 0
      start_time = Time.now.utc # we start timer after file is read from S3

      avro.each do |hash|
        # TODO sample raw logger
        # if counter % 10_000 == 0
        # if counter == 0
        #   logger.info hash.inspect
        # end

        result = case type
        when :track
          track(hash, event_name)
        when :session
          store_session(hash)
        when :alias
          store_alias(hash)
        when :manual_alias # useful for manually processing user_migrations as aliases
          aliaz(hash)
        else
          send(type, hash)
        end

        skipped += 1 if result.nil?
        counter += 1
      end

      diff = Time.now.utc - start_time
      if diff > 0
        logger.info "Done. Loading #{load_diff.to_i}s, processing #{diff.to_i}s, #{counter} rows, #{skipped} skipped (#{(counter / diff).to_i} rows/sec)"
      end
    end

    def parse_time(time)
      Time.zone.parse(time).utc
    end

    def parse_heap_timestamp(value)
      return unless value
      Time.at(value / 1_000_000).utc.iso8601
    rescue
      value
    end

    def wrap_cookie(heap_user_id, resolve=true)
      return nil unless heap_user_id

      resolved_user_id = resolve ? resolve_heap_user(heap_user_id) : heap_user_id
      "#{@project_identifier}|#{resolved_user_id}"
    end

    def common_payload(hash)
      heap_user_id = hash.delete('user_id')
      {
        anonymous_id: wrap_cookie(heap_user_id),
        message_id: "HEAP|#{hash.delete('event_id')}",
        timestamp: parse_time(hash.delete('time')),
        context: {
          'ip' => (hash.delete('ip') || hash.delete('browser_ip')),
          'library' => {
            'name' => 'HeapIntegration',
            'version' => '1.0'
          }
        },
        properties: {
          'heap_user_id' => heap_user_id
        }
      }
    end

    def skip_before?(timestamp)
      @skip_before && timestamp < @skip_before
    end

    def track(hash, event_name)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:event] = event_name
      payload[:properties].merge!(hash.reject { |_, v| v.nil? || v.to_s.bytesize > 200 })

      if revenue_field = @revenue_mapping[event_name]
        payload[:properties]['revenue'] ||= hash.delete(revenue_field.to_s)
      else @revenue_fallback.any?
        payload[:properties]['revenue'] ||= hash.values_at(*@revenue_fallback).compact.first
      end

      @processor.track(payload)
    end

    def page(hash)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:name] = 'Loaded a Page'

      # TODO detect mobile and send screen event instead
      url = case hash['library']
      when 'web'
        'https://' + hash.values_at('domain', 'path', 'query', 'hash').join
      when 'ios', 'android'
        "#{hash['library']}-app://" + hash.values_at('app_name', 'view_controller').compact.join('/')
      else
        'unknown://' + hash['event_id'].to_s
      end

      # UPDATE 2025-04-18 probably that doesn't exist anymore or was custom
      # if `session_time` present detect session referrer in place
      referrer = if hash['session_time']
        session_time = parse_time(hash.delete('session_time'))
        if session_time == payload[:timestamp]
          hash.delete('referrer')
        end
      end

      # UPDATE 2025-04-18 new logic to detect session start
      referrer ||= if @session_cache.has_key?(hash['session_id']) && (@session_cache[hash['session_id']] == payload[:timestamp])
        hash.delete('referrer')
      end

      # build previous page if referrer not found previously
      previous_page = hash.delete('previous_page') || hash.delete('heap_previous_page')
      if !referrer && previous_page
        referrer = if hash['domain'].present?
          'https://' + hash['domain'] + previous_page
        else # weird cases when previous_page is something like local path e.g. "/C:/Users/bk221/OneDrive/Desktop/NFL%20Draft%20Big%20Board.html"
          'unknown://' + previous_page
        end
      end

      payload[:properties] = {
        'referrer' => referrer,
        'title' => hash.delete('title'),
        'url' => url
      }

      @processor.page(payload)
    end

    def identify(hash)
      heap_user_id = hash.delete('user_id')
      email = hash.delete('email') || hash.delete('_email')
      identity = hash.delete('identity')

      # common workaround for heap? email used as identity
      if email.nil? && identity && identity.include?('@')
        email = identity
      end

      # uses email as USER_ID or sets it to null (if email is empty)
      if @user_id_prop == 'email'
        identity = email
      end

      payload = {
        anonymous_id: wrap_cookie(heap_user_id),
        user_id: identity,
        traits: {
          'email' => email,
          'identity' => identity,
          'heap_user_id' => heap_user_id,
          'join_date' => parse_heap_timestamp(hash.delete('joindate')),
          'last_modified' => parse_heap_timestamp(hash.delete('last_modified'))
        }.reject { |_, v| v.nil? }
      }

      payload[:traits] = hash.reject { |_, v| v.nil? }.merge(payload[:traits])

      return if @identify_only_users && payload[:user_id].nil?

      if @alias_on_identify
        # OLD approach - works slower on big user migrations but consumes less memory
        # @alias_cache.
        #   select { |_, v| v == heap_user_id }.
        #   keys.
        if @alias_cache_reverse.has_key?(heap_user_id)
          @alias_cache_reverse[heap_user_id].each do |from_user_id|
            alias_payload = {
              'from_user_id' => from_user_id,
              'to_user_id'   => heap_user_id
            }
            aliaz(alias_payload)
          end
        end
      end

      @processor.identify(payload)
    end

    def aliaz(hash)
      payload = {
        previous_id:  wrap_cookie(hash['from_user_id'], false),
        anonymous_id: wrap_cookie(hash['to_user_id'],   false)
      }
      p payload if @prompt

      @processor.alias(payload)
    end

    def store_session(hash)
      @session_cache[hash['session_id']] = parse_time(hash['time'])
    end

    def store_alias(hash)
      if @alias_on_identify
        @alias_cache_reverse[hash['to_user_id']] ||= Set.new
        @alias_cache_reverse[hash['to_user_id']].add(hash['from_user_id'])
      end

      @alias_cache[hash['from_user_id']] ||= hash['to_user_id']
    end

    def resolve_heap_user(heap_user_id)
      @alias_cache[heap_user_id] || heap_user_id
    end

    def s3uri_to_hash(s3uri)
      raise ArgumentError unless s3uri[0..4] == 's3://'

      bucket, key = s3uri[5..-1].split('/', 2)
      { bucket: bucket, key: key }
    end

    def s3_get_file(obj)
      hash = case obj
      when String
        s3uri_to_hash(obj)
      when Aws::S3::Types::Object
        { bucket: @aws_s3_bucket, key: obj.key }
      when Hash
        obj
      else
        {}
      end

      raise ArgumentError unless hash.has_key?(:bucket) && hash.has_key?(:key)

      @s3.get_object(hash)
    end

  end
end
