defmodule TLake.TaskConsumer do
  use ConsumerSupervisor

  @moduledoc false

  def start_link(max_demand) do
    ConsumerSupervisor.start_link(__MODULE__, max_demand, name: __MODULE__)
  end

  def init(max_demand) do
    children = [%{id: TLake.GenDAG, start: {TLake.GenDAG, :start_link, []}, restart: :transient}]

    opts = [
      strategy: :one_for_one,
      subscribe_to: [{TLake.TaskProducer, max_demand: max_demand, min_demand: 1}]
    ]

    ConsumerSupervisor.init(children, opts)
  end
end
