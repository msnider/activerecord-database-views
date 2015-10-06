module ActiveRecord::DatabaseViews
  class View
    FILE_NAME_MATCHER_WITH_PREFIX = /^\d+?_(.+)/

    attr_reader :path

    def initialize(path)
      @path = path
    end

    def drop!
      if materialize? then
        call_sql!("DROP MATERIALIZED VIEW IF EXISTS #{name} CASCADE;")
      else
        call_sql!("DROP VIEW IF EXISTS #{name} CASCADE;")
      end
    end

    def load!
      if materialize? then
        call_sql!("CREATE MATERIALIZED VIEW #{name} AS #{sql};")
      else
        call_sql!("CREATE OR REPLACE VIEW #{name} AS #{sql};")
      end
    end

    def name
      if basename =~ FILE_NAME_MATCHER_WITH_PREFIX
        FILE_NAME_MATCHER_WITH_PREFIX.match(basename)[1]
      else
        basename
      end
    end

    private

    def basename
      @basename ||= File.basename(path, '.sql').chomp('.mat')
    end

    def materialize?
      @materialize ||= File.basename(path, '.sql').end_with?('.mat')
    end

    def full_path
      Rails.root.join(path)
    end

    def sql
      File.read(full_path)
    end

    def call_sql!(sql)
      ActiveRecord::Base.connection.execute(sql)
    end
  end
end
