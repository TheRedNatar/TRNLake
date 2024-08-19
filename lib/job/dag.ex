defmodule TLake.Job.DAG do
  require Logger
  @moduledoc false

  def start(root_path, target_date, server_map, options) do
    aws_config = %{
      bucket: Application.fetch_env!(:t_lake, :aws_bucket),
      region: Application.fetch_env!(:t_lake, :aws_region),
      access_key_id: Application.fetch_env!(:t_lake, :aws_access_key_id),
      secret_access_key: Application.fetch_env!(:t_lake, :aws_secret_access_key)
    }

    kaggle_bin = Application.fetch_env!(:t_lake, :kaggle_bin)

    options = Map.merge(options, %{kaggle_bin: kaggle_bin})

    with(
      {:ok, server_starting_date_s} <- Map.fetch(server_map, :start_date),
      {:ok, server_name} <- Map.fetch(server_map, :name),
      {:ok, server_starting_date} = Date.from_iso8601(server_starting_date_s),
      {:ok, server_url} <- Map.fetch(server_map, :url),
      server_id = TLake.Job.Utils.gen_server_id(server_url, server_starting_date),
      f_filename = TLake.Job.Utils.f_filename(root_path, aws_config, server_id),
      options = Map.merge(options, %{start_date: server_starting_date, server_name: server_name}),
      {1, :ok} <- collect_raw_snapshot(f_filename, target_date, server_id, server_map, options),
      {2, :ok} <- {2, TLake.Job.Snapshot.run(f_filename, target_date, server_id, options)},
      {3, :ok} <- {3, pull_raw_snapshots_from_s3(f_filename, target_date, server_id, options)},
      {4, :ok} <-
        {4, TLake.Job.SnapshotsToKaggle.run(f_filename, target_date, server_id, options)}
    ) do
      :ok
    else
      error -> error
    end
  end

  def collect_raw_snapshot(f_filename, target_date, server_id, server_map, options) do
    with(
      {1, :ok} <-
        {1, TLake.Job.RawSnapshot.run(f_filename, target_date, server_id, server_map, options)},
      {2, :ok} <- {2, TLake.Job.RawSnapshotToS3.run(f_filename, target_date, server_id, options)}
    ) do
      :ok
    else
      error -> error
    end
  end

  def pull_raw_snapshots_from_s3(
        f_filename,
        target_date,
        server_id,
        options = %{server_starting_date: server_starting_date}
      ) do
    Enum.to_list(Date.range(server_starting_date, target_date))
    |> Enum.each(fn date ->
      Logger.debug(%{
        is_cloud?: true,
        msg: "Attempt cloud pulling date #{date} from #{server_id}",
        server_id: server_id,
        target_date: target_date
      })

      case TLake.Job.RawSnapshotFromS3.run(f_filename, date, server_id, options) do
        :ok ->
          Logger.info(%{
            is_cloud?: true,
            msg: "Cloud pulled date #{date} from #{server_id}",
            server_id: server_id,
            target_date: target_date
          })

          TLake.Job.Snapshot.run(f_filename, date, server_id, options)

        error ->
          error
      end
    end)

    :ok
  end
end
