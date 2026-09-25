# Picks a read node for each GET. Writes stay on the leader.
class Cluster
  READ_ROLES = %i[reading_n2 reading_n3].freeze
  LABELS = {
    writing: "n1 127.0.0.1:3306",
    reading_n2: "n2 127.0.0.1:3307",
    reading_n3: "n3 127.0.0.1:3308"
  }.freeze

  def self.next_read_role
    @mutex ||= Mutex.new
    @mutex.synchronize do
      @index = @index.to_i + 1
      READ_ROLES[@index % READ_ROLES.size]
    end
  end
end
