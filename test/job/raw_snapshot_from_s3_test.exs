defmodule TLake.Job.RawSnapshotFromS3Test do
  use ExUnit.Case
  require Explorer.DataFrame, as: DF

  @moduletag :cloud

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

  @tag :tmp_dir
  test "download valid raw_snapshot", %{tmp_dir: root_path} do
    target_date = ~D[2024-05-29]
    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config(), server_id())
    options = %{}

    assert :ok == TLake.Job.RawSnapshotFromS3.run(f_filename, target_date, server_id(), options)
    {:ok, local_fss} = f_filename.({TLake.Job.RawSnapshot.table_name(), target_date})
    assert File.exists?(local_fss.path)

    df = DF.from_parquet!(local_fss, lazy: false)
    assert DF.n_rows(df) == 25471
    assert DF.n_columns(df) == 14
  end

  @tag :tmp_dir
  test "download invalid raw_snapshot", %{tmp_dir: root_path} do
    target_date = ~D[2024-05-28]
    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config(), server_id())
    options = %{}

    {atom, _} = TLake.Job.RawSnapshotFromS3.run(f_filename, target_date, server_id(), options)
    assert :error == atom
  end
end
