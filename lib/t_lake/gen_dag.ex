defmodule TLake.GenDAG do
  use GenServer

  @moduledoc false

  @spec start_link(event :: {binary(), Date.t(), map(), map()}) :: GenServer.on_start()
  def start_link({root_path, utc_1_date, server_map, options}),
    do: GenServer.start_link(__MODULE__, {root_path, utc_1_date, server_map, options})

  @impl true
  def init(state) do
    {:ok, state, {:continue, :init_dag}}
  end

  @impl true
  def handle_continue(:init_dag, state = {root_path, utc_1_date, server_map, options}) do
    case TLake.Job.DAG.start(root_path, utc_1_date, server_map, options) do
      :ok -> {:stop, :normal, {:ok, state}}
      error -> {:stop, :normal, {:error, error, state}}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
