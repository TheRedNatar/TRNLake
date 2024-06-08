defmodule TLake.GenDAG do
  use GenServer

  @spec start_link(event :: {binary(), Date.t(), map()}) :: GenServer.on_start()
  def start_link({root_path, utc_1_date, server_map}),
    do: GenServer.start_link(__MODULE__, {root_path, utc_1_date, server_map})

  @impl true
  def init(state) do
    {:ok, state, {:continue, :init_dag}}
  end

  @impl true
  def handle_continue(:init_dag, state = {root_path, utc_1_date, server_map}) do
    case TLake.Job.DAG.start(root_path, utc_1_date, server_map) do
      :ok -> {:stop, :normal, {:ok, state}}
      error -> {:stop, :normal, {:error, error, state}}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
