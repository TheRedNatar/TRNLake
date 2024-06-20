defmodule TLake.TaskProducer do
  use GenStage

  @moduledoc false

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec send_bulk_server_maps(server_maps :: [{binary(), Date.t(), map(), map()}]) :: :ok
  def send_bulk_server_maps(server_maps) do
    GenStage.call(__MODULE__, {:bulk_server_maps, server_maps})
  end

  @impl true
  def init(:ok) do
    {:producer, :ok, dispatcher: GenStage.BroadcastDispatcher}
  end

  @impl true
  def handle_call({:bulk_server_maps, server_maps}, _from, state) do
    {:reply, :ok, server_maps, state}
  end

  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end
