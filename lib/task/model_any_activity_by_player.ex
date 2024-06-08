defmodule TLake.Job.ModelAnyActivityByPlayer do
  alias Explorer.Series
  alias TLake.Job.Transformations, as: T
  alias TLake.Job.Utils
  require Explorer.DataFrame, as: DF

  def schema() do
    [
      {"server_id", :string},
      {"target_date", :date},
      {"player_id", :u32},
      {"prediction_any_increase?", :f32}
    ]
  end

  def run(f_filename, target_date, server_id, %{n_days: n_days}) do
    dfs =
      [df_daily | _] =
      Utils.read_parquets(
        f_filename,
        TLake.Job.Snapshot.table_name(),
        Enum.map(0..n_days, &Date.add(target_date, -&1))
      )

    df_input = DF.concat_rows(dfs)

    df_activity = T.any_increase_by_player(df_input)

    df =
      df_daily
      |> DF.group_by("player_id")
      |> DF.summarise_with(
        &[
          total_population: Series.sum(&1["population"]),
          total_villages: Series.count(&1["village_id"]),
          speed: Series.first(&1["speed"]),
          day_of_week: Series.first(Series.day_of_week(&1["target_date"])),
          days_since_started: Series.first(T.date_diff(&1["target_date"], &1["start_date"]))
        ]
      )
      |> DF.join(df_activity, on: ["player_id"], how: :left)
      |> DF.collect()

    model = 1

    prediction = EXGBoost.predict(model, to_tensor(df))

    df_output =
      df
      |> DF.mutate_with(fn _ldf ->
        [
          server_id: Utils.lit(server_id, :binary),
          target_date: Utils.lit(target_date, :date),
          prediction_any_increase?: Series.from_tensor(prediction, :f32)
        ]
      end)
      |> DF.select([
        "server_id",
        "target_date",
        "player_id",
        "prediction_any_increase?"
      ])
  end

  defp to_tensor(df) do
    Nx.tensor([
      Series.to_list(df["any_increase?"]),
      Series.to_list(df["total_population"]),
      Series.to_list(df["total_villages"]),
      Series.to_list(df["speed"]),
      Series.to_list(df["day_of_week"]),
      Series.to_list(df["days_since_started"])
    ])
    |> Nx.transpose()
  end
end
