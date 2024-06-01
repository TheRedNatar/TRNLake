defmodule TLake.Job.RawSnapshot do
  alias Explorer.DataFrame
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  alias TLake.Job.Utils

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
      df = DF.new(binary_record: String.split(raw_snapshot, "\n", trim: true)),
      df = DF.mutate_with(df, add_extra_info(server_id, target_date, server_map)),
      {:ok, filename} <- f_filename.(table_name()),
      :ok <-
        DF.to_parquet(df, filename,
          compression: {:brotli, 11},
          streaming: false
        )
    ) do
      :ok
    end
  end

  defp add_extra_info(server_id, target_date, server_map) do
    column_pairs = [
      name: Utils.lit(server_map[:name], :string),
      version: Utils.lit(server_map[:version], :string),
      speed: Utils.lit(server_map[:speed], {:u, 8}),
      url: Utils.lit(server_map[:url]),
      number_of_tribes: Utils.lit(server_map[:number_of_tribes], {:u, 8}),
      start_date: Utils.lit(server_map[:start_date], :date),
      timezone: Utils.lit(server_map[:timezone], :string),
      timezone_offset: Utils.lit(server_map[:timezone_offset], {:s, 8}),
      artifacts_date: Utils.lit(server_map[:artifacts_date], :date),
      building_plans_date: Utils.lit(server_map[:building_plans_date], :date),
      end_date: Utils.lit(:erlang.atom_to_binary(server_map[:end_date]), :string),
      server_id: Utils.lit(server_id, :binary),
      target_date: Utils.lit(target_date, :date)
    ]

    fn _ldf -> column_pairs end
  end
end
