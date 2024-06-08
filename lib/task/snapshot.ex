defmodule TLake.Job.Snapshot do
  require Explorer.DataFrame, as: DF

  def table_name(), do: "snapshot"
  def table_name_error(), do: "snapshot_error"
  def partitions(server_id, target_date), do: [server_id: server_id, target_date: target_date]

  def schema() do
    [
      {"server_id", :string},
      {"target_date", :date},
      {"alliance_id", :u32},
      {"alliance_name", :string},
      {"grid_position", :u32},
      {"has_harbor", :boolean},
      {"is_capital", :boolean},
      {"is_city", :boolean},
      {"player_id", :u32},
      {"player_name", :string},
      {"population", :u16},
      {"region", :string},
      {"tribe", :u8},
      {"victory_points", :u32},
      {"village_id", :u32},
      {"village_name", :string},
      {"x_position", :s32},
      {"y_position", :s32},
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

  def input_schema() do
    [
      {"alliance_id", :u32},
      {"alliance_name", :string},
      {"grid_position", :u32},
      {"has_harbor", :boolean},
      {"is_capital", :boolean},
      {"is_city", :boolean},
      {"player_id", :u32},
      {"player_name", :string},
      {"population", :u16},
      {"region", :string},
      {"tribe", :u8},
      {"victory_points", :u32},
      {"village_id", :u32},
      {"village_name", :string},
      {"x_position", :s16},
      {"y_position", :s16}
    ]
  end

  def run(f_filename, target_date, _server_id, _opts) do
    with(
      {:ok, input_filename} <- f_filename.({TLake.Job.RawSnapshot.table_name(), target_date}),
      {:ok, df_input} <- DF.from_parquet(input_filename),
      {df_ok, df_error} = process(df_input),
      {:ok, output_ok_filename} <- f_filename.({table_name(), target_date}),
      {:ok, output_error_filename} <- f_filename.({table_name_error(), target_date}),
      :ok <- DF.to_parquet(df_ok, output_ok_filename),
      :ok <- DF.to_parquet(df_error, output_error_filename)
    ) do
      :ok
    end
  end

  defp process(df_input) do
    df = parse_binary_records(df_input)

    df_error =
      DF.filter(df, col("parse_ok?") == false) |> DF.discard(["parse_ok?", "binary_record"])

    df_ok = DF.filter(df, col("parse_ok?") == true) |> DF.discard(["parse_ok?", "binary_record"])
    {df_ok, df_error}
  end

  defp parse_binary_records(df) do
    new_rows = for row <- DF.to_rows(df), do: parse_binary_record(row)
    DF.new(new_rows, dtypes: schema())
  end

  defp parse_binary_record(row) do
    binary_record = Map.fetch!(row, "binary_record")

    case :travianmap_map.parse_line(binary_record) do
      {:ok, new_row} -> Map.merge(row, new_row) |> Map.merge(%{"parse_ok?" => true})
      _ -> Map.merge(row, fake_nil_map()) |> Map.merge(%{"parse_ok?" => false})
    end
  end

  defp fake_nil_map() do
    %{
      "alliance_id" => nil,
      "alliance_name" => nil,
      "grid_position" => nil,
      "has_harbor" => nil,
      "is_capital" => nil,
      "is_city" => nil,
      "player_id" => nil,
      "player_name" => nil,
      "population" => nil,
      "region" => nil,
      "tribe" => nil,
      "victory_points" => nil,
      "village_id" => nil,
      "village_name" => nil,
      "x_position" => nil,
      "y_position" => nil
    }
  end
end
