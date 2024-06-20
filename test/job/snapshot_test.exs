defmodule TLakeTest.Job.Snapshot do
  use ExUnit.Case
  doctest TLake

  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  def compare_2_dfs(df_good, df_outptut) do
    good_n_rows = DF.n_rows(df_good)

    n_rows =
      DF.concat_rows([df_good, df_outptut])
      |> DF.group_by(DF.names(df_good))
      |> DF.summarise_with(&[n: Series.size(&1["village_id"])])
      |> DF.filter_with(&[Series.equal(&1["n"], 2)])
      |> DF.n_rows()

    assert good_n_rows == n_rows
  end

  @tag :tmp_dir
  test "end2end resource input", %{tmp_dir: root_path} do
    File.cp_r!(
      Path.join([
        Application.fetch_env!(:t_lake, :test_resources),
        "snapshot_end2end_resource_input"
      ]),
      root_path
    )

    target_date = ~D[2024-06-16]
    server_id = "ts2.x1.arabics.travian.com_20230919"

    aws_config = %{
      bucket: "fake_bucket",
      region: "fake_region",
      access_key_id: "fake_access_key_id",
      secret_access_key: "fake_secret_access_key"
    }

    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config, server_id)

    assert :ok == TLake.Job.Snapshot.run(f_filename, target_date, server_id, %{})

    {:ok, filename_snapshot_test} = f_filename.({"test_snapshot", target_date})
    {:ok, filename_snapshot_error_test} = f_filename.({"test_snapshot_error", target_date})
    {:ok, filename_snapshot_output} = f_filename.({"snapshot", target_date})
    {:ok, filename_snapshot_error_output} = f_filename.({"snapshot_error", target_date})

    df_snapshot_test = DF.from_parquet!(filename_snapshot_test)
    df_snapshot_error_test = DF.from_parquet!(filename_snapshot_error_test)

    df_snapshot_output = DF.from_parquet!(filename_snapshot_output)
    df_snapshot_error_output = DF.from_parquet!(filename_snapshot_error_output)

    assert compare_2_dfs(df_snapshot_test, df_snapshot_output)
    assert compare_2_dfs(df_snapshot_error_test, df_snapshot_error_output)
  end
end
