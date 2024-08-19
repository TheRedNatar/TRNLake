# defmodule TLake.Job.ModelAnyActivityByPlayer do
#   alias Explorer.Series
#   alias TLake.Job.Transformations, as: T
#   alias TLake.Job.Utils
#   require Explorer.DataFrame, as: DF

#   @moduledoc false

#   def schema() do
#     [
#       {"server_id", :string},
#       {"target_date", :date},
#       {"player_id", :u32},
#       {"prediction_any_increase?", :f32}
#     ]
#   end

#   def run(f_filename, target_date, _server_id, %{n_days: n_days}) do
#     dfs =
#       [df_daily | _] =
#       Utils.read_parquets(
#         f_filename,
#         TLake.Job.Snapshot.table_name(),
#         Enum.map(0..n_days, &Date.add(target_date, -&1))
#       )

#     df_input = DF.concat_rows(dfs)

#     df_activity = T.any_increase_by_player(df_input)

#     df =
#       df_daily
#       |> DF.group_by("player_id")
#       |> DF.summarise_with(
#         &[
#           total_population: Series.sum(&1["population"]),
#           total_villages: Series.count(&1["village_id"]),
#           speed: Series.first(&1["speed"]),
#           day_of_week: Series.first(Series.day_of_week(&1["target_date"])),
#           days_since_started: Series.first(T.date_diff(&1["target_date"], &1["start_date"]))
#         ]
#       )
#       |> DF.join(df_activity, on: ["player_id"], how: :left)
#       |> DF.collect()

#     df

#     # model = 1

#     # prediction = EXGBoost.predict(model, to_tensor(df))

#     # df_output =
#     #   df
#     #   |> DF.mutate_with(fn _ldf ->
#     #     [
#     #       server_id: Utils.lit(server_id, :binary),
#     #       target_date: Utils.lit(target_date, :date),
#     #       prediction_any_increase?: Series.from_tensor(prediction, :f32)
#     #     ]
#     #   end)
#     #   |> DF.select([
#     #     "server_id",
#     #     "target_date",
#     #     "player_id",
#     #     "prediction_any_increase?"
#     #   ])
#   end

#   def process_input(df_0, df_1, df_2, df_3) do
#     df_s_1 =
#       DF.group_by(df_1, "player_id")
#       |> DF.summarise_with(&process_snapshot/1)
#       |> DF.rename_with(fn col_name -> "#{col_name}_1" end)
#       |> DF.rename(%{"player_id_1" => "player_id"})

#     df_s_2 =
#       DF.group_by(df_2, "player_id")
#       |> DF.summarise_with(&process_snapshot/1)
#       |> DF.rename_with(fn col_name -> "#{col_name}_2" end)
#       |> DF.rename(%{"player_id_2" => "player_id"})

#     df_s_3 =
#       DF.group_by(df_3, "player_id")
#       |> DF.summarise_with(&process_snapshot/1)
#       |> DF.rename_with(fn col_name -> "#{col_name}_3" end)
#       |> DF.rename(%{"player_id_3" => "player_id"})

#     df_act_0 =
#       T.any_increase_by_player(df_0, df_1)
#       |> DF.rename(%{"any_increase?" => "any_increase_0?"})

#     df_act_1 =
#       T.any_increase_by_player(df_1, df_2)
#       |> DF.rename(%{"any_increase?" => "any_increase_1?"})

#     df_act_2 =
#       T.any_increase_by_player(df_2, df_3)
#       |> DF.rename(%{"any_increase?" => "any_increase_2?"})

#     df_0
#     |> DF.group_by("player_id")
#     |> DF.summarise_with(
#       &[
#         total_population: Series.sum(&1["population"]),
#         total_villages: Series.count(&1["village_id"]),
#         min_population: Series.min(&1["population"]),
#         mean_population: Series.mean(&1["population"]),
#         median_population: Series.median(&1["population"]),
#         speed: Series.first(&1["speed"]),
#         day_of_week: Series.first(Series.day_of_week(&1["target_date"])),
#         days_since_server_started: Series.first(T.date_diff(&1["target_date"], &1["start_date"]))
#       ]
#     )
#     |> DF.join(df_act_0, on: ["player_id"], how: :left)
#     |> DF.join(df_act_1, on: ["player_id"], how: :left)
#     |> DF.join(df_act_2, on: ["player_id"], how: :left)
#     |> DF.join(df_s_1, on: ["player_id"], how: :left)
#     |> DF.join(df_s_2, on: ["player_id"], how: :left)
#     |> DF.join(df_s_3, on: ["player_id"], how: :left)
#     |> DF.mutate_with(
#       &[
#         player_started_day_0: Series.is_nil(&1["any_increase_0?"]),
#         player_started_day_1: Series.is_nil(&1["any_increase_1?"]),
#         player_started_day_2: Series.is_nil(&1["any_increase_2?"])
#       ]
#     )
#     |> DF.mutate_with(
#       &[
#         any_increase_0?: Series.fill_missing(&1["any_increase_0?"], true),
#         any_increase_1?: Series.fill_missing(&1["any_increase_1?"], true),
#         any_increase_2?: Series.fill_missing(&1["any_increase_2?"], true)
#       ]
#     )
#   end

#   defp process_snapshot(df_grouped_by_player) do
#     [
#       total_population: Series.sum(df_grouped_by_player["population"]),
#       total_villages: Series.count(df_grouped_by_player["village_id"]),
#       min_population: Series.min(df_grouped_by_player["population"]),
#       mean_population: Series.mean(df_grouped_by_player["population"]),
#       median_population: Series.median(df_grouped_by_player["population"])
#     ]
#   end

#   def to_tensor(df_input) do
#     Nx.stack(df_input, axis: 1)
#   end
# end
