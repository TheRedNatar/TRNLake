defmodule TLake.Job.DAGTest do
  use ExUnit.Case

  setup_all do
    Application.stop(:t_lake)
  end

  def aws_config() do
    %{
      bucket: Application.fetch_env!(:t_lake, :aws_bucket),
      region: Application.fetch_env!(:t_lake, :aws_region),
      access_key_id: Application.fetch_env!(:t_lake, :aws_access_key_id),
      secret_access_key: Application.fetch_env!(:t_lake, :aws_secret_access_key)
    }
  end

  def server_id(), do: "alpler.x1.tr.travian.com_20231102"

  @tag :cloud
  @tag timeout: 1000 * 120
  @tag :tmp_dir
  test "download available snapshots to local from s3", %{tmp_dir: root_path} do
    target_date = ~D[2024-06-02]
    server_starting_date = ~D[2024-05-10]
    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config(), server_id())
    options = %{server_starting_date: server_starting_date}

    assert :ok ==
             TLake.Job.DAG.pull_raw_snapshots_from_s3(
               f_filename,
               target_date,
               server_id(),
               options
             )

    dates = Date.range(server_starting_date, target_date)

    available_in_s3 = [
      ~D[2024-05-29],
      ~D[2024-05-30],
      ~D[2024-05-31],
      ~D[2024-06-01]
    ]

    pulled_from_s3 =
      for date <- dates,
          File.exists?(elem(f_filename.({TLake.Job.RawSnapshot.table_name(), date}), 1).path),
          do: date

    assert available_in_s3 == pulled_from_s3
  end
end
