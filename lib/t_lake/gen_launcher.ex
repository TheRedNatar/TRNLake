defmodule TLake.GenLauncher do
  use GenServer

  @moduledoc false

  @launch_time ~T[00:08:00]
  @relaunch_interval 1000 * 60 * 3
  @hour_in_milis 1000 * 60 * 60
  @day_in_milis 1000 * 60 * 60 * 24

  @spec start_link() :: GenServer.on_start()
  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    {:ok, %{tref: nil}, {:continue, :init_start}}
  end

  @impl true
  def handle_continue(:init_start, state) do
    launch_and_wait(state)
  end

  @impl true
  def handle_info(:launch_now, state) do
    launch_and_wait(state)
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp launch_and_wait(state) do
    root_path = Application.fetch_env!(:t_lake, :root_path)
    utc_1_date = utc_1_date()
    aws_enable = Application.get_env(:t_lake, :aws_enable, false)

    options = %{aws_enable: aws_enable}

    case :travianmap.get_servers() do
      {:error, _reason} = _error ->
        tref = :erlang.send_after(@relaunch_interval, self(), :launch_now)
        {:noreply, %{state | tref: tref}}

      {:ok, servers} ->
        server_maps =
          for {atom, server_map} <- servers,
              atom == :ok,
              do: {root_path, utc_1_date, server_map, options}

        TLake.TaskProducer.send_bulk_server_maps(Enum.shuffle(server_maps))

        tref = :erlang.send_after(milis_to_next_launch(@launch_time), self(), :launch_now)
        {:noreply, %{state | tref: tref}}
    end
  end

  @spec milis_to_next_launch(launch_time :: Time.t()) :: non_neg_integer()
  def milis_to_next_launch(launch_time) do
    utc_1_now = DateTime.utc_now() |> DateTime.add(@hour_in_milis, :millisecond)

    case Time.diff(launch_time, DateTime.to_time(utc_1_now), :millisecond) do
      0 -> 0
      x when x > 0 -> x
      x when x < 0 -> @day_in_milis + x
    end
  end

  defp utc_1_date() do
    DateTime.utc_now()
    |> DateTime.add(@hour_in_milis, :millisecond)
    |> DateTime.to_date()
  end
end
