defmodule TLake.Job.Transformations do
  alias Explorer.DataFrame
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  @moduledoc false

  @spec date_diff(s1 :: Series.t(), s2 :: Series.t()) :: Series.t()
  def date_diff(s1, s2) do
    Series.subtract(s1, s2)
    |> Series.divide(1000 * 60 * 60 * 24)
    |> Series.cast({:u, 16})
  end

  @spec any_increase_by_player(df :: DataFrame.t()) :: DataFrame.t()
  def any_increase_by_player(df) do
    df
    |> DF.group_by(["player_id", "village_id"])
    |> DF.sort_with(&[{:desc, &1["target_date"]}])
    |> DF.mutate_with(
      &[
        population_increment_by_village:
          Series.greater(Series.add(Series.shift(&1["population"], 1), -&1["population"]), 0)
      ]
    )
    |> DF.summarise_with(&[any_increase?: Series.any?(&1["population_increment_by_village"])])
    |> DF.group_by(["player_id"])
    |> DF.summarise_with(&[any_increase?: Series.any?(&1["any_increase_by_village"])])
  end
end
