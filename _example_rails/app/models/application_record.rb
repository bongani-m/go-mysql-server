class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class

  connects_to database: {
    writing: :primary,
    reading_n2: :replica_n2,
    reading_n3: :replica_n3
  }
end
