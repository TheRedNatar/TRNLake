defmodule TLake.Job.DAG do
  def start(root_path, target_date, server_map) do
    aws_config = %{
      bucket: Application.fetch_env!(:t_lake, :aws_bucket),
      region: Application.fetch_env!(:t_lake, :aws_region),
      access_key_id: Application.fetch_env!(:t_lake, :aws_access_key_id),
      secret_access_key: Application.fetch_env!(:t_lake, :aws_secret_access_key)
    }

    with(
      {:ok, server_starting_date_s} <- Map.fetch(server_map, :start_date),
      {:ok, server_starting_date} = Date.from_iso8601(server_starting_date_s),
      {:ok, server_url} <- Map.fetch(server_map, :url),
      server_id = TLake.Job.Utils.gen_server_id(server_url, server_starting_date),
      f_filename = TLake.Job.Utils.f_filename(root_path, aws_config, server_id),
      :ok <- TLake.Job.RawSnapshot.run(f_filename, target_date, server_id, server_map, %{}),
      :ok <- TLake.Job.RawSnapshotToS3.run(f_filename, target_date, server_id, %{}),
      :ok <- TLake.Job.Snapshot.run(f_filename, target_date, server_id, %{})
    ) do
      :ok
    else
      error -> error
    end
  end
end
