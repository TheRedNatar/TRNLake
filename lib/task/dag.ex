defmodule TLake.Job.DAG do
  def start(root_path, target_date, server_map) do
    server_starting_date = Date.from_iso8601!(server_map[:start_date])
    server_url = server_map[:url]
    server_id = TLake.Job.Utils.gen_server_id(server_url, server_starting_date)

    aws_config = %{
      bucket: Application.fetch_env!(:t_lake, :aws_bucket),
      region: Application.fetch_env!(:t_lake, :aws_region),
      access_key_id: Application.fetch_env!(:t_lake, :aws_access_key_id),
      secret_access_key: Application.fetch_env!(:t_lake, :aws_secret_access_key)
    }

    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config, server_id, target_date)
    TLake.Job.RawSnapshot.run(f_filename, target_date, server_id, server_map, [])
    TLake.Job.RawSnapshotToS3.run(f_filename, target_date, server_id, [])
  end
end
