class ApplicationController < ActionController::Base
  around_action :route_cluster

  private

  # GET/HEAD read from n2 and n3, except the request right after a write.
  # That request still has a flash, and the replicas can lag the leader.
  def route_cluster
    role = if request.get? || request.head?
      flash.empty? ? Cluster.next_read_role : :writing
    else
      :writing
    end
    Current.node = Cluster::LABELS.fetch(role)
    ActiveRecord::Base.connected_to(role: role) { yield }
  end
end
