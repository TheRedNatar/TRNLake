defmodule TLake.Job.RawSnapshot do
  require Explorer.DataFrame, as: DF
  require Explorer.Series, as: S

  alias TLake.Job.Utils

  @moduledoc false

  def table_name(), do: "raw_snapshot"
  def partitions(server_id, target_date), do: [server_id: server_id, target_date: target_date]

  def schema() do
    [
      {"binary_record", :string},
      {"server_id", :string},
      {"target_date", :date},
      {"artifacts_date", :date},
      {"building_plans_date", :date},
      {"end_date", :string},
      {"name", :string},
      {"number_of_tribes", {:u, 8}},
      {"speed", {:u, 8}},
      {"start_date", :date},
      {"timezone", :string},
      {"timezone_offset", {:s, 8}},
      {"url", :string},
      {"version", :string}
    ]
  end

  # aws_path, server_map
  def run(f_filename, target_date, server_id, server_map, _opts) do
    with(
      {:ok, raw_snapshot} <- :travianmap.get_map(server_map[:url]),
      false <- empty_raw_string?(raw_snapshot),
      df = process(raw_snapshot, server_id, target_date, server_map),
      {:ok, filename} <- f_filename.({table_name(), target_date})
    ) do
      DF.to_parquet(df, filename, compression: {:brotli, 11}, streaming: false)
    end
  end

  @spec process(
          raw_snapshot :: String.t(),
          server_id :: String.t(),
          target_date :: Date.t(),
          server_map :: %{atom() => any()}
        ) ::
          DF.t()
  def process(raw_snapshot, server_id, target_date, server_map) do
    DF.new(binary_record: String.split(raw_snapshot, "\n", trim: true))
    |> DF.mutate_with(add_server_map_info(server_id, target_date, server_map))
    |> DF.mutate_with(&cast_dates_from_strings/1)
  end

  defp empty_raw_string?(""), do: true
  defp empty_raw_string?(_raw_snapshot_string), do: false

  defp add_server_map_info(server_id, target_date, server_map) do
    column_pairs = [
      name: Utils.lit(server_map[:name], :string),
      version: Utils.lit(server_map[:version], :string),
      speed: Utils.lit(server_map[:speed], {:u, 8}),
      url: Utils.lit(server_map[:url]),
      number_of_tribes: Utils.lit(server_map[:number_of_tribes], {:u, 8}),
      start_date: Utils.lit(server_map[:start_date], :string),
      timezone: Utils.lit(server_map[:timezone], :string),
      timezone_offset: Utils.lit(server_map[:timezone_offset], {:s, 8}),
      artifacts_date: Utils.lit(server_map[:artifacts_date], :string),
      building_plans_date: Utils.lit(server_map[:building_plans_date], :string),
      end_date: Utils.lit(:erlang.atom_to_binary(server_map[:end_date]), :string),
      server_id: Utils.lit(server_id, :binary),
      target_date: Utils.lit(target_date, :date)
    ]

    fn _ldf -> column_pairs end
  end

  defp cast_dates_from_strings(ldf) do
    [
      start_date: S.cast(ldf["start_date"], :date),
      artifacts_date: S.cast(ldf["artifacts_date"], :date),
      building_plans_date: S.cast(ldf["building_plans_date"], :date)
    ]
  end
end
