defmodule TLake.Job.Transformations do
  alias Explorer.DataFrame
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  @spec compute_activity(df: DataFrame.t()) :: DataFrame.t()
  def compute_activity(df) do
    grouped = DF.group_by(df, ["player_id", "village_id"])
    grouped_sorted = DF.sort_by(grouped, desc: target_date)

    grouped_sorted_shifted =
      DF.mutate(grouped_sorted,
        population_increment_by_village: shift(population, 1) - population > 0
      )

    village_activity =
      DF.summarise(grouped_sorted_shifted,
        any_increase_by_village: any?(population_increment_by_village)
      )

    village_activity_grouped = DF.group_by(village_activity, "player_id")
    DF.summarise(village_activity_grouped, any_increase?: any?(any_increase_by_village))
  end

  @spec date_diff(s1 :: Series.t(), s2 :: Series.t()) :: Series.t()
  def date_diff(s1, s2) do
    Series.cast(Series.divide(Series.subtract(s1, s2), 1000 * 60 * 60 * 24), :u16)
  end

  def any_increase_by_player(df) do
    df
    |> DF.group_by(["player_id", "village_id"])
    |> DF.sort_with(
      &[
        {:desc, &1["target_date"]}
      ]
    )
    |> DF.mutate_with(
      &[
        population_increment_by_village: Series.shift(&1["population"], 1) - &1["population"] > 0
      ]
    )
    |> DF.summarise_with(
      &[
        any_increase?: Series.any?(&1["population_increment_by_village"])
      ]
    )
    |> DF.group_by(["player_id"])
    |> DF.summarise_with(
      &[
        any_increase?: Series.any?(&1["any_increase_by_village"])
      ]
    )
  end
end
