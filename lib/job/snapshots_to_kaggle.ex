defmodule TLake.Job.SnapshotsToKaggle do
  require Explorer.DataFrame, as: DF

  alias TLake.Job.Utils

  @moduledoc false

  def run(_, _, _, %{kaggle_enable: false}), do: :ok

  def run(f_filename, target_date, server_id, %{
        start_date: start_date,
        server_name: server_name,
        kaggle_bin: kaggle_bin
      }) do
    dfs =
      Utils.read_parquets(
        f_filename,
        TLake.Job.Snapshot.table_name(),
        Enum.map(0..Date.diff(target_date, start_date), &Date.add(target_date, -&1))
      )

    df =
      DF.concat_rows(dfs)
      |> DF.discard(["timezone", "timezone_offset"])
      |> DF.relocate(columns_order(), before: 0)

    with(
      {:ok, tmp_dir} <- tmp_dir(server_id),
      tmp_filename = Path.join(tmp_dir, "snapshots.csv"),
      IO.puts(tmp_filename),
      :ok <- DF.to_csv(df, tmp_filename),
      :ok <- push_to_kaggle(tmp_dir, kaggle_bin, server_name, start_date, target_date),
      # Delete on else may be better
      {:ok, _} <- File.rm_rf(tmp_dir)
    ) do
      :ok
    end
  end

  defp push_to_kaggle(
         folder_name,
         kaggle_bin_path,
         server_name,
         server_starting_date,
         target_date
       ) do
    with(
      {:ok, template} <-
        TLake.Job.SnapshotsToKaggle.metadata_template(
          :init,
          server_name,
          server_starting_date,
          "therednatar"
        ),
      :ok <- File.write(Path.join(folder_name, "dataset-metadata.json"), template),
      {kaggle_msg, 0} <-
        System.cmd(kaggle_bin_path, ["datasets", "create", "--public", "--path", folder_name])
    ) do
      case String.contains?(kaggle_msg, "Dataset creation error:") do
        true ->
          push_version_to_kaggle(
            folder_name,
            kaggle_bin_path,
            server_name,
            server_starting_date,
            target_date
          )

        false ->
          :ok
      end
    end
  end

  defp push_version_to_kaggle(
         folder_name,
         kaggle_bin_path,
         server_name,
         server_starting_date,
         target_date
       ) do
    with(
      {:ok, template} <-
        TLake.Job.SnapshotsToKaggle.metadata_template(
          :version,
          server_name,
          server_starting_date,
          "therednatar"
        ),
      :ok <- File.write(Path.join(folder_name, "dataset-metadata.json"), template),
      {kaggle_msg, 0} <-
        System.cmd(kaggle_bin_path, [
          "datasets",
          "version",
          "--path",
          folder_name,
          "--message",
          "Updated in #{Date.to_iso8601(target_date)}"
        ])
    ) do
      case String.contains?(kaggle_msg, "Dataset creation error:") do
        true -> {:error, kaggle_msg}
        false -> :ok
      end
    end
  end

  defp tmp_dir(server_id) do
    case System.tmp_dir() do
      nil ->
        {:error, :nil_dir}

      tmp ->
        tmp_dir = Path.join([tmp, "trn_lake_kaggle_dir", server_id])

        case File.mkdir_p(tmp_dir) do
          :ok -> {:ok, tmp_dir}
          error -> error
        end
    end
  end

  defp columns_order() do
    [
      "grid_position",
      "x_position",
      "y_position",
      "tribe",
      "village_id",
      "village_name",
      "player_id",
      "player_name",
      "alliance_id",
      "alliance_name",
      "population",
      "has_harbor",
      "is_capital",
      "is_city",
      "region",
      "victory_points",
      "server_id",
      "target_date",
      "name",
      "number_of_tribes",
      "speed",
      "start_date",
      "url",
      "version",
      "artifacts_date",
      "building_plans_date",
      "end_date"
    ]
  end

  def metadata_template(status, server_name, server_start_date, kaggle_username) do
    template = %{
      title: "Map.sql--#{server_name}--#{server_start_date}",
      subtitle: "Daily map.sql snapshot collection from Travian and cleaned by TheRedNatar",
      description:
        "This dataset contains serveral snapshots (one per day) with every village public information plus some server information like starting date or speed. It is aggregate by village, so each row is the snapshot of the village in date target_date.
        The primary key of the table is the village_id + target_date.


        Take a view on this notebook in order to understand the content and usage of this file https://www.kaggle.com/code/therednatar/travian-map-sql-exploration
        ",
      id: "#{kaggle_username}/#{String.replace(server_name, " ", "-")}--#{server_start_date}",
      licenses: [%{name: "CC0-1.0"}],
      keywords: [
        "business",
        "beginner",
        "tabular",
        "games",
        "video games"
      ],
      resources: [
        %{
          path: "snapshots.csv",
          description: "File containing all the snapshots collected since server starting date",
          schema: %{
            fields: [
              %{name: "grid_position", type: "integer", title: "Position in the grid"},
              %{name: "x_position", type: "integer", title: "X position on the map"},
              %{name: "y_position", type: "integer", title: "Y position on the map"},
              %{name: "tribe", type: "integer", title: "Tribe identifier"},
              %{name: "village_id", type: "integer", title: "Identifier for the village"},
              %{name: "village_name", type: "string", title: "Name of the village"},
              %{name: "player_id", type: "integer", title: "Identifier for the player"},
              %{name: "player_name", type: "string", title: "Name of the player"},
              %{name: "alliance_id", type: "integer", title: "Identifier for the alliance"},
              %{name: "alliance_name", type: "string", title: "Name of the alliance"},
              %{name: "population", type: "integer", title: "Population size"},
              %{name: "has_harbor", type: "boolean", title: "Indicates if there is a harbor"},
              %{name: "is_capital", type: "boolean", title: "Indicates if it is the capital"},
              %{name: "is_city", type: "boolean", title: "Indicates if it is a city"},
              %{name: "region", type: "string", title: "Region name"},
              %{name: "victory_points", type: "integer", title: "Victory points scored"},
              %{name: "server_id", type: "string", title: "Server unique identifier"},
              %{
                name: "target_date",
                type: "string",
                title: "Map.sql collected date, just right after midnigh"
              },
              %{name: "name", type: "string", title: "Server name"},
              %{name: "number_of_tribes", type: "integer", title: "Number of tribes available"},
              %{name: "speed", type: "integer", title: "Server game speed"},
              %{name: "start_date", type: "string", title: "Server start date"},
              %{name: "url", type: "string", title: "Server url"},
              %{name: "version", type: "string", title: "Travian server version"},
              %{name: "artifacts_date", type: "string", title: "Artifacts release date"},
              %{
                name: "building_plans_date",
                type: "string",
                title: "Building plans release date"
              },
              %{
                name: "end_date",
                type: "string",
                title: "Server end date or WW if ends by wonder of the world"
              }
            ]
          }
        }
      ]
    }

    case status do
      :init ->
        Jason.encode(template)

      :version ->
        Map.drop(template, [:title, :licenses])
        |> Jason.encode()
    end
  end
end
